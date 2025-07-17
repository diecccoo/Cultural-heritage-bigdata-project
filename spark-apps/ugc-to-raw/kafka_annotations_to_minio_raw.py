# This script:
# Reads JSON messages from the Kafka topic called user_annotations.
# Writes data (in JSON format) to MinIO in the raw layer.
# Partitions by dt=YYYY-MM-DD, saves 1 file per batch, every 30 seconds.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, lit

print("[INFO] SparkSession initialization with MinIO access...")
spark = SparkSession.builder \
    .appName("KafkaToMinIO_RAW_JSON") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
print("[INFO] SparkSession created.")

print("[INFO] Connection to Kafka (topic: user_annotations)...")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_annotations") \
    .option("startingOffsets", "latest") \
    .load()

print("[INFO] Parsing and adding metadata...")
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as value") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("kafka:user_annotations")) \
    .withColumn("dt", current_date())

print("[INFO] Start JSON write to MinIO (coalesce(1), batch every 30s)...")
query = df_parsed.coalesce(1).writeStream \
    .format("json") \
    .option("path", "s3a://heritage/raw/metadata/user_generated_content/") \
    .option("checkpointLocation", "s3a://heritage/raw/checkpoints/user_annotations_json/") \
    .partitionBy("dt") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("[INFO] Writing initiated. Pending events...")
query.awaitTermination()
