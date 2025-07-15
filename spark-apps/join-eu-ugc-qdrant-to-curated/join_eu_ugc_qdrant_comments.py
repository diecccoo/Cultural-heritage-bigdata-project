from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, first
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import time
import pandas as pd

# in ugc c'è object_id
# in qdrant c'è id_object
# in europeana c'è guid 

# =================== CONFIG ===================
UGC_PATH = "s3a://heritage/cleansed/user_generated/"
EUROPEANA_PATH = "s3a://heritage/cleansed/europeana/"
CURATED_PATH = "s3a://heritage/curated/join_metadata_deduplicated/"

QDRANT_HOST = "qdrant"
QDRANT_PORT = 6333
QDRANT_COLLECTION = "heritage_embeddings"

RELOAD_EUROPEANA_MIN = 1
RELOAD_QDRANT_MIN = 1

# =================== FUNZIONI ===================
def get_qdrant_mapping():
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    results = []
    offset = None
    while True:
        batch, offset = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(must=[FieldCondition(key="status", match=MatchValue(value="validated"))]),
            limit=1000,
            with_payload=True
        )
        results.extend(batch)
        if offset is None:
            break

    points_data = [p.payload for p in results if p.payload]
    df = pd.DataFrame(points_data)

    if "canonical_id" not in df.columns or "id_object" not in df.columns:
        return pd.DataFrame(columns=["id_object", "canonical_id", "guid"])

    # Trova il primo id_object per ciascun canonical_id
    first_ids = df.groupby("canonical_id")["id_object"].first().reset_index()
    first_ids = first_ids.rename(columns={"id_object": "guid"})

    # Join per ottenere una mappa: ogni id_object → suo guid canonicale
    mapping_df = df[["id_object", "canonical_id"]].merge(first_ids, on="canonical_id", how="left")

    return mapping_df[["id_object", "guid"]]  # guid sarà usato per il join Europeana

def get_latest_processed_timestamp(spark):
    try:
        df_curated = spark.read.format("delta").load(CURATED_PATH)
        max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
        return max_ts
    except Exception:
        return None

def read_latest_delta_table(spark, path):
    return spark.read.format("delta").load(path)

# =================== SPARK ===================
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

# =================== MAIN LOOP ===================
last_europeana_reload = 0
last_qdrant_reload = 0
europeana_df = None
mapping_df = None

print("[DEBUG] Job avviato: join duplicati Qdrant → metadati Europeana")

while True:
    now = time.time()

    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        last_europeana_reload = now

    if mapping_df is None or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        mapping_pd = get_qdrant_mapping()
        mapping_df = spark.createDataFrame(mapping_pd)
        last_qdrant_reload = now

    latest_ts = get_latest_processed_timestamp(spark)

    ugc_df = spark.read.format("delta").load(UGC_PATH)
    if latest_ts:
        ugc_df = ugc_df.filter(col("timestamp") > lit(latest_ts))

    # Join per sostituire id_object con guid canonicale
    # 1. Rinomina per join
    ugc_df = ugc_df.withColumnRenamed("object_id", "id_object")

    # 2. Join con mapping Qdrant → ottieni il canonical guid
    ugc_with_guid = ugc_df.join(mapping_df, on="id_object", how="inner") \
                        .drop("id_object")

    # 🔍 Ora `ugc_with_guid` ha un campo `guid` pronto per il join con Europeana

    print(f"[DEBUG] Annotazioni trovate con mapping Qdrant: {ugc_with_guid.count()}")

    # Join finale con Europeana (su guid)
    joined_df = ugc_with_guid.join(europeana_df, on="guid", how="inner") \
                             .withColumn("joined_at", current_timestamp())

    print(f"[DEBUG] Righe totali dopo il join Europeana: {joined_df.count()}")

    if joined_df.count() > 0:
        joined_df.write.format("delta").mode("append").save(CURATED_PATH)
        print("[DEBUG] ✅ Scrittura completata nel layer curated.")
    else:
        print("[DEBUG] ❌ Nessuna riga da scrivere.")

    print("[DEBUG] Attendo 60 secondi prima del prossimo ciclo...")
    time.sleep(60)
