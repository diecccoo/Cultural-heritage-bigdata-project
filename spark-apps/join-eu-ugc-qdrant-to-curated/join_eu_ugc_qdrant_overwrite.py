from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, first
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import time

# =================== CONFIG ===================
# MinIO paths
UGC_PATH = "s3a://heritage/cleansed/user_generated/"
EUROPEANA_PATH = "s3a://heritage/cleansed/europeana/"
CURATED_PATH = "s3a://heritage/curated/join_metadata_deduplicated/"

# Qdrant config
QDRANT_HOST = "qdrant"  
QDRANT_PORT = 6333
QDRANT_COLLECTION = "heritage_embeddings"

# Reload intervals (in minutes)
RELOAD_EUROPEANA_MIN = 1
RELOAD_QDRANT_MIN = 1

# =============================================

def get_validated_ids_from_qdrant():
    """
    Query Qdrant for all points with status = 'validated'.
    Return a set of unique guid from the first point of each canonical_id group.
    """
    print("[DEBUG] Connection to Qdrant and recovery of validated points...")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=20)

    # Scroll all points with status = "validated"
    results = []
    offset = None

    while True:
        batch, offset = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[FieldCondition(key="status", match=MatchValue(value="validated"))]
            ),
            limit=1000,
            with_payload=True
        )
        results.extend(batch)
        if offset is None:
            break

    print(f"[DEBUG] Trovati {len(results)} punti con status=validated")

    # Convert to DataFrame-style dict list
    import pandas as pd
    points_data = [point.payload for point in results if point.payload]
    df = pd.DataFrame(points_data)

    # Group by canonical_id and take the first guid
    if 'canonical_id' not in df.columns or 'guid' not in df.columns:
        print("[DEBUG WARN] Canonical_id or guid fields not found in Qdrant payloads.")
        return set()

    grouped = df.groupby("canonical_id").first().reset_index()
    validated_ids = set(grouped["guid"].tolist())
    print(f"[DEBUG] Total deduplicated guid (one per canonical_id): {len(validated_ids)}")
    # print("[DEBUG] Examples of guid from Qdrant (first 10):")
    # for id_ in list(validated_ids)[:10]:
    #     print(f" - {id_}")
    return validated_ids


def get_latest_processed_timestamp(spark):
    """
    Retrieves the maximum timestamp already processed by the curated layer.
    """
    try:
        df_curated = spark.read.format("delta").load(CURATED_PATH)
        max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
        print(f"[DEBUG] Maximum timestamp found in the curated layer: {max_ts}")
        return max_ts
    except Exception as e:
        print(f"[DEBUG] No timestamp found: curated table may not exist yet. Error detail: {str(e)}")
        return None


def read_latest_delta_table(spark, path):
    return spark.read.format("delta").load(path)


# ========== SPARK SESSION ==========
builder = SparkSession.builder \
    .appName("Join_EU_UGC_Qdrant_Curated") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ========== INITIALIZATION ==========
last_europeana_reload = 0
last_qdrant_reload = 0
europeana_df = None
validated_ids = None

print("[DEBUG] Job started: join with Qdrant filter + Delta")

while True:
    now = time.time()

    # Reload Europeana if necessary
    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        print("[DEBUG] Europeana metadata reload from Delta...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        last_europeana_reload = now

    # Reload validated IDs from Qdrant if necessary
    if validated_ids is None or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        validated_ids = get_validated_ids_from_qdrant()
        last_qdrant_reload = now


    
    # Filter Europeana on validated_ids from Qdrant
    filtered_europeana_df = europeana_df.filter(col("guid").isin(list(validated_ids)))
    # print("[DEBUG] Example filtered Europeana metadata (guid, title se presente):")
    # if "title" in filtered_europeana_df.columns:
    #     filtered_europeana_df.select("guid", "title").show(5, truncate=False)
    # else:
    #     filtered_europeana_df.select("guid").show(5, truncate=False)

    # print(f"[DEBUG] List guid filtered from Qdrant: {validated_ids}")
    print(f"[DEBUG] Europeana total before filter: {europeana_df.count()}")
    print(f"[DEBUG] Europeana filtered after Qdrant: {filtered_europeana_df.count()}")



    # Trova l'ultimo timestamp processato
    latest_ts = get_latest_processed_timestamp(spark)
    print(f"[DEBUG] Ultimo timestamp processato in curated: {latest_ts}")

    # Leggi annotazioni da UGC (rimuovi il filtro latest_ts qui)
    ugc_df = spark.read.format("delta").load(UGC_PATH)
    if latest_ts:
        ugc_df = ugc_df.filter(col("timestamp") > lit(latest_ts))
    
    try:
        if ugc_df.rdd.isEmpty():
            print("[DEBUG] UGC è vuoto: nessun dato da mostrare.")
        # else:
        #     print("[DEBUG] Esempio valori UGC (guid, timestamp):")
        #     ugc_df.select("guid", "timestamp").show(5, truncate=False)
    except Exception as e:
        print(f"[DEBUG ERROR] Errore nel tentativo di mostrare UGC: {str(e)}")


    ugc_count = ugc_df.count()
    if ugc_count == 0:
        print("[DEBUG] Nessuna nuova annotazione trovata. Attendo...")
        time.sleep(75)
        continue
    else:
        print(f"[DEBUG] Annotazioni nuove trovate: {ugc_count}")

        ugc_ids = set([r["guid"] for r in ugc_df.select("guid").distinct().collect()])
        europeana_ids = set([r["guid"] for r in filtered_europeana_df.select("guid").distinct().collect()])
        intersection = ugc_ids.intersection(europeana_ids)

    ugc_ids = set([r["guid"] for r in ugc_df.select("guid").distinct().collect()])
    europeana_ids = set([r["guid"] for r in filtered_europeana_df.select("guid").distinct().collect()])

    intersection = ugc_ids.intersection(europeana_ids)
    print(f"[DEBUG] Oggetti in comune tra Europeana filtrata e UGC: {len(intersection)}")
    print(f"[DEBUG] UGC count: {ugc_df.count()}")
    print(f"[DEBUG] Europeana filtrata count: {filtered_europeana_df.count()}")

    print("[DEBUG] Eseguo join tra UGC e Europeana (filtrata Qdrant)...")
    joined_df = ugc_df.join(filtered_europeana_df, on="guid", how="right") \
                      .withColumn("joined_at", current_timestamp())
   
    print(f"[METRICHE] Annotazioni totali: {ugc_df.count()}")
    print(f"[METRICHE] Oggetti Europeana filtrati (presenti in Qdrant): {filtered_europeana_df.count()}")
    print(f"[METRICHE] Annotazioni collegate (dopo join): {joined_df.count()}")
    print(f"[METRICHE] Annotazioni escluse (senza canonical_id valido): {ugc_df.count() - joined_df.count()}")


    row_count = joined_df.count()
    print(f"[DEBUG] Join completato. Righe da scrivere: {row_count}")

    if row_count > 0:
        joined_df.write.format("delta").mode("overwrite").save(CURATED_PATH)
        print("[DEBUG] Scrittura completata nel layer curated (overwrite).")

    print("[DEBUG] Attendo 45 secondi prima del prossimo ciclo...")
    time.sleep(45)
