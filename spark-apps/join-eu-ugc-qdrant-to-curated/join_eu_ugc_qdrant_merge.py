from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, first
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import time
from delta.tables import DeltaTable # Importa DeltaTable per la funzionalità MERGE


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
    print("[DEBUG] Connessione a Qdrant e recupero punti 'validated'...")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=20)

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

    import pandas as pd
    points_data = [point.payload for point in results if point.payload]
    df = pd.DataFrame(points_data)

    if 'canonical_id' not in df.columns or 'guid' not in df.columns:
        print("[DEBUG WARN] Campi canonical_id o guid non trovati nei payload Qdrant.")
        return set()

    grouped = df.groupby("canonical_id").first().reset_index()
    validated_ids = set(grouped["guid"].tolist())
    print(f"[DEBUG] Totale guid deduplicati (un per canonical_id): {len(validated_ids)}")
    return validated_ids

def get_latest_processed_timestamp(spark_session):
    """
    Recupera il timestamp massimo già processato dal layer curated.
    """
    try:
        if DeltaTable.isDeltaTable(spark_session, CURATED_PATH):
            df_curated = spark_session.read.format("delta").load(CURATED_PATH)
            max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
            print(f"[DEBUG] Timestamp massimo trovato nel layer curated: {max_ts}")
            return max_ts
        else:
            print("[DEBUG] La tabella Delta in CURATED_PATH non esiste ancora.")
            return None
    except Exception as e:
        print(f"[DEBUG] Errore nel recupero del timestamp massimo: {str(e)}. La tabella potrebbe essere vuota o non esistere.")
        return None

def read_latest_delta_table(spark_session, path):
    return spark_session.read.format("delta").load(path)

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

print("[DEBUG] Job avviato: join con filtro Qdrant + Delta (con MERGE per scrittura)")

while True:
    now = time.time()

    # Reload Europeana if necessary
    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        print("[DEBUG] Ricarico metadati Europeana da Delta...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        last_europeana_reload = now

    # Reload validated IDs from Qdrant if necessary
    if validated_ids is None or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        validated_ids = get_validated_ids_from_qdrant()
        last_qdrant_reload = now

    # Filtro Europeana su validated_ids da Qdrant
    # NOTA: Se validated_ids è vuoto, filtered_europeana_df sarà vuoto.
    if validated_ids:
        filtered_europeana_df = europeana_df.filter(col("guid").isin(list(validated_ids)))
    else:
        print("[DEBUG] Nessun ID validato da Qdrant. filtered_europeana_df sarà vuoto.")
        filtered_europeana_df = spark.createDataFrame([], europeana_df.schema) # Crea un DF vuoto con lo stesso schema
    
    print(f"[DEBUG] Europeana totali prima del filtro: {europeana_df.count()}")
    print(f"[DEBUG] Europeana filtrati dopo Qdrant: {filtered_europeana_df.count()}")

    # Trova l'ultimo timestamp processato dal layer curated
    latest_ts = get_latest_processed_timestamp(spark)
    print(f"[DEBUG] Ultimo timestamp processato in curated: {latest_ts}")

    # Leggi annotazioni da UGC.
    ugc_df = spark.read.format("delta").load(UGC_PATH)

    # Questo filtro è cruciale per l'elaborazione incrementale efficiente.
    if latest_ts:
        ugc_df = ugc_df.filter(col("timestamp") > lit(latest_ts))
    
    ugc_count = ugc_df.count()
    if ugc_count == 0:
        print("[DEBUG] Nessuna nuova annotazione trovata. Attendo...")
        time.sleep(60)
        continue
    else:
        print(f"[DEBUG] Annotazioni nuove/da processare trovate: {ugc_count}")

    # Esegui join tra UGC e Europeana (filtrata Qdrant)
    print("[DEBUG] Eseguo join tra UGC e Europeana (filtrata Qdrant)...")
    
    # Esegui il join. Le colonne di UGC e Europeana saranno nel joined_df.
    # Le colonne 'guid', 'timestamp' e 'user_id' (dal lato UGC) saranno usate come chiave per il MERGE.
    joined_df = ugc_df.join(filtered_europeana_df, on="guid", how="right") \
                      .withColumn("joined_at", current_timestamp())
    
    # DEBUG: Verifica duplicati nella joined_df prima della deduplicazione e del MERGE
    print("[DEBUG] Verifica duplicati nella joined_df prima della deduplicazione (chiave: guid, user_id, timestamp)...")
    duplicate_source_check = joined_df.groupBy("guid", "user_id", "timestamp").count() \
                                     .filter("count > 1")
    
    if duplicate_source_check.count() > 0:
        print("⚠️ ERRORE: La sorgente joined_df contiene duplicati per (guid, user_id, timestamp) PRIMA della deduplicazione!")
        duplicate_source_check.show(truncate=False)
    else:
        print("✅ La sorgente joined_df è unica per (guid, user_id, timestamp) prima della deduplicazione.")

    # DEDUPLICAZIONE: Rimuove i duplicati dalla sorgente basandosi sulla chiave di merge
    # Questo è fondamentale perché il MERGE non deduplica la sorgente, ma gestisce gli upsert sulla destinazione.
    print("[DEBUG] Eseguo deduplicazione della joined_df sulla chiave (guid, user_id, timestamp)...")
    
    # Per semplicità, manteniamo la prima occorrenza trovata in caso di duplicati esatti.
    # Se avessi bisogno di una logica più complessa (es. tenere l'ultima modifica),
    # useresti una Window function con orderBy prima di filtrare per row_number() == 1.
    joined_df_deduplicated = joined_df.dropDuplicates(["guid", "user_id", "timestamp"])
    
    print(f"[DEBUG] Righe nella joined_df originale: {joined_df.count()}")
    print(f"[DEBUG] Righe nella joined_df deduplicata: {joined_df_deduplicated.count()}")

    print(f"[METRICHE] Annotazioni totali (da UGC): {ugc_count}")
    print(f"[METRICHE] Oggetti Europeana filtrati (presenti in Qdrant): {filtered_europeana_df.count()}")
    print(f"[METRICHE] Righe dopo il join e filtro (prima del MERGE): {joined_df_deduplicated.count()} (dopo deduplicazione)")
    
    # Gestione della scrittura nel layer curato con MERGE
    row_count = joined_df_deduplicated.count()
    print(f"[DEBUG] Join e deduplicazione completati. Righe da scrivere tramite MERGE: {row_count}")

    if row_count > 0:
        # Verifica se la tabella Delta esiste, altrimenti la crea
        if not DeltaTable.isDeltaTable(spark, CURATED_PATH):
            print(f"[DEBUG] La tabella Delta in '{CURATED_PATH}' non esiste. Creazione iniziale della tabella.")
            # La prima scrittura crea la tabella con lo schema di joined_df.
            # Usiamo 'append' per questa prima scrittura. Tutte le esecuzioni successive useranno il MERGE.
            joined_df_deduplicated.write.format("delta").mode("append").save(CURATED_PATH)
            print(f"[DEBUG] Tabella Delta creata e primo batch scritto in '{CURATED_PATH}'.")
        else:
            deltaTable = DeltaTable.forPath(spark, CURATED_PATH)

            print("[DEBUG] Esegui MERGE nel layer curated, inserendo solo nuove annotazioni...")
            deltaTable.alias("target") \
              .merge(
                joined_df_deduplicated.alias("source"),
                "target.guid = source.guid AND target.timestamp = source.timestamp AND target.user_id = source.user_id"
              ) \
              .whenNotMatchedInsertAll() \
              .execute()
            print("[DEBUG] Scrittura completata nel layer curated tramite MERGE (solo inserimento nuove righe).")
    else:
        print("[DEBUG] Nessuna riga da scrivere tramite MERGE.")

    print("[DEBUG] Attendo 30 secondi prima del prossimo ciclo...")
    time.sleep(30)