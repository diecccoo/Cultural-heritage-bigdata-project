from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Create SparkSession with S3A (MinIO) support
spark = SparkSession.builder \
    .appName("ScannerConsumer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Define the expected schema of the incoming JSON
schema = StructType() \
    .add("scanId", StringType()) \
    .add("uri", StringType()) \
    .add("timestamp", StringType()) \
    .add("mime", StringType())

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "new-scans") \
    .load()

# Parse JSON from Kafka value
parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write to MinIO in Parquet format
query = parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("path", "s3a://europeana-data/parquet/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
