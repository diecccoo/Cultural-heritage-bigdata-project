from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max
from delta import configure_spark_with_delta_pip
import time

### Configurazione dei path MinIO (usando s3a://)
UGC_PATH = "s3a://heritage/cleansed/user_generated/"
EUROPEANA_PATH = "s3a://heritage/cleansed/europeana/"
CURATED_PATH = "s3a://heritage/curated/join_metadata/"

# Funzione di utilità per leggere la tabella Delta più recente
def read_latest_delta_table(spark, path):
    return spark.read.format("delta").load(path)

# Funzione per determinare l'ultimo timestamp processato da curated
def get_latest_processed_timestamp(spark):
    try:
        df_curated = spark.read.format("delta").load(CURATED_PATH)
        max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
        return max_ts
    except:
        return None  # La tabella potrebbe non esistere ancora

# Inizializza la sessione Spark con configurazione MinIO e Delta Lake
builder = SparkSession.builder \
    .appName("Join_Europeana_UGC_Curated") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Ricarica periodica della tabella Europeana ogni 15 minuti
RELOAD_INTERVAL_MIN = 15
last_europeana_reload = 0

europeana_df = None
print("[INFO] Job avviato: join Delta tables Europeana + UGC")

while True:
    now = time.time()

    # Ricarica Europeana se necessario
    if europeana_df is None or (now - last_europeana_reload > RELOAD_INTERVAL_MIN * 60):
        print("[INFO] Ricarico Europeana da Delta cleansed...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        last_europeana_reload = now

    # Trova l'ultimo timestamp processato in curated
    latest_ts = get_latest_processed_timestamp(spark)
    print(f"[INFO] Ultimo timestamp processato: {latest_ts}")

    # Leggi solo i nuovi commenti UGC
    ugc_df = spark.read.format("delta").load(UGC_PATH)
    if latest_ts:
        ugc_df = ugc_df.filter(col("timestamp") > lit(latest_ts))

    # Rinomina 'object_id' in 'guid' per permettere il join
    ugc_df = ugc_df.withColumnRenamed("object_id", "guid")

    if ugc_df.isEmpty():
        print("[INFO] Nessun nuovo commento trovato. Attendo il prossimo ciclo...")
        time.sleep(60)
        continue

    print("[INFO] Eseguo join tra UGC nuovi e metadati Europeana...")
    joined_df = ugc_df.join(europeana_df, on="guid", how="inner") \
                      .withColumn("joined_at", current_timestamp())

    print(f"[INFO] Scrivo {joined_df.count()} righe nel layer curated (Delta)...")
    joined_df.write.format("delta") \
        .mode("append") \
        .save(CURATED_PATH)

    print("[INFO] Join completato. Attendo 60 secondi...")
    time.sleep(60)  # Micro-batch ogni 60 secondi
