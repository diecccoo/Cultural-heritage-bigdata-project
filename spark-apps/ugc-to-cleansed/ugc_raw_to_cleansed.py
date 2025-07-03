from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp
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
    StructField("object_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
])

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

print("[INFO] Parsing completato. Ecco lo schema del dataframe:")
df_parsed.printSchema()

# === RIMOZIONE DUPLICATI (object_id + timestamp) ===
df_dedup = df_parsed.dropDuplicates(["object_id", "user_id", "comment", "timestamp"])
# annotazioni duplicate (magari per errore di rete, replay Kafka, ecc.) vengono dallo stesso utente, nello stesso istante

# === SCRITTURA IN DELTA FORMAT CON COALESCE(1) ===
print("[INFO] Scrittura del dataframe in formato Delta nel cleansed layer...")
(
    df_dedup
    .coalesce(1)
    .write
    .format("delta")
    .mode("append")
    .option("compression", "snappy")
    .save(CLEANSED_PATH)
)

print("[INFO] Job completato con successo.")
spark.stop()
