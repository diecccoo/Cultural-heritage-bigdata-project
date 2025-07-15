from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp, max
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


# ---------------- SparkSession configurata per MinIO + Delta ----------------
spark = SparkSession.builder \
    .appName("CleansUGC Raw JSON to Cleansed Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === CONFIGURAZIONE ===
RAW_PATH = "s3a://heritage/raw/metadata/user_generated_content/"
CLEANSED_PATH = "s3a://heritage/cleansed/user_generated/"
SOURCE_NAME = "kafka:user_annotations"

# === SCHEMA DEI DATI JSON ALL'INTERNO DI "value" ===
ugc_schema = StructType([
    StructField("guid", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
])

# === CARICA TIMESTAMP MASSIMO GIA' SCRITTO ===
try:
    df_existing = spark.read.format("delta").load(CLEANSED_PATH)
    last_ts = df_existing.select(max("timestamp_cleansed")).first()[0]
    print(f"[INFO] Ultimo timestamp_cleansed presente in Delta: {last_ts}")
except Exception as e:
    last_ts = None
    print(f"[INFO] Nessun file Delta trovato, procedo da zero: {e}")

# === LETTURA DATI GREZZI ===
print("[INFO] Inizio lettura dei file JSON dal raw layer...")
df_raw = spark.read.json(RAW_PATH)
print(f"[INFO] File letti: {df_raw.count()} record totali.")

# === PARSING DEL CAMPO 'value' E MANTENIMENTO METADATI ===
df_parsed = (
    df_raw
    .withColumn("parsed", from_json(col("value"), ugc_schema))
    .select(
        col("parsed.*"),
        col("ingestion_time"),
        col("source")
    )
)

# === FILTRO SUI NUOVI DATI ===
if last_ts:
    df_filtered = df_parsed.filter(col("ingestion_time") > lit(last_ts))
else:
    df_filtered = df_parsed


df_filtered = df_filtered.withColumn("timestamp_cleansed", current_timestamp())
# === RIMOZIONE DUPLICATI (guid + timestamp) ===
df_dedup = df_filtered.dropDuplicates(["guid", "user_id", "comment", "timestamp"])
# annotazioni duplicate (magari per errore di rete, replay Kafka, ecc.) vengono dallo stesso utente, nello stesso istante

# === SCRITTURA IN DELTA FORMAT CON COALESCE(1) ===
count_new = df_dedup.count()
if count_new > 0:
    (
        df_dedup
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .option("compression", "snappy")
        .save(CLEANSED_PATH)
    )
    print(f"[✅] Scritti {count_new} nuovi record in cleansed.")
else:
    print("[🟡] Nessun nuovo record da scrivere.")

spark.stop()
