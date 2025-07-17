from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp, max
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


# ---------------- SparkSession configured for MinIO + Delta ----------------
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

# === CONFIGURATION ===
RAW_PATH = "s3a://heritage/raw/metadata/user_generated_content/"
CLEANSED_PATH = "s3a://heritage/cleansed/user_generated/"
SOURCE_NAME = "kafka:user_annotations"

# === SCHEMA OF JSON DATA INSIDE “value” ===
ugc_schema = StructType([
    StructField("guid", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
])

# === UPLOAD MAXIMUM TIMESTAMP ALREADY WRITTEN ===
try:
    df_existing = spark.read.format("delta").load(CLEANSED_PATH)
    last_ts = df_existing.select(max("timestamp_cleansed")).first()[0]
    print(f"[INFO] Last timestamp_cleansed present in Delta: {last_ts}")
except Exception as e:
    last_ts = None
    print(f"[INFO] No Delta found: {e}")

# === RAW DATA READING ===
print("[INFO] Starting the reading of JSON files from raw layer...")
df_raw = spark.read.json(RAW_PATH)
print(f"[INFO] File read: {df_raw.count()} records.")

# === PARSING OF THE ‘value’ FIELD AND METADATA MAINTENANCE ===
df_parsed = (
    df_raw
    .withColumn("parsed", from_json(col("value"), ugc_schema))
    .select(
        col("parsed.*"),
        col("ingestion_time"),
        col("source")
    )
)

# === FILTER ON NEW DATA ===
if last_ts:
    df_filtered = df_parsed.filter(col("ingestion_time") > lit(last_ts))
else:
    df_filtered = df_parsed


df_filtered = df_filtered.withColumn("timestamp_cleansed", current_timestamp())
# === DUPLICATE REMOVAL (guid + timestamp) ===
df_dedup = df_filtered.dropDuplicates(["guid", "user_id", "comment", "timestamp"])
# duplicate annotations (perhaps by network error, Kafka replay, etc.) come from the same user, at the same time

# === DELTA FORMAT WRITING WITH COALESCE(1) ===
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
    print(f"Written {count_new} new records in CLEANSED.")
else:
    print("We don't have new records to write.")

spark.stop()
