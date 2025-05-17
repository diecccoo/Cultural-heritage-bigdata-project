from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# SparkSession with MinIO (S3A) support
spark = SparkSession.builder \
    .appName("JoinScannerAndMetadata") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Schema for scanner stream
scanner_schema = StructType() \
    .add("scanId", StringType()) \
    .add("guid", StringType()) \
    .add("uri", StringType()) \
    .add("timestamp", StringType()) \
    .add("mime", StringType())

# Schema for Europeana metadata
meta_schema = StructType() \
    .add("guid", StringType()) \
    .add("title", StringType()) \
    .add("creator", StringType()) \
    .add("type", StringType()) \
    .add("rights", StringType()) \
    .add("timestamp_created", StringType()) \
    .add("dataProvider", StringType())

# Read scanner stream
scanner_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "new-scans") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), scanner_schema).alias("scanner")) \
    .select("scanner.*")

# Read Europeana metadata stream
meta_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "europeana-metadata") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), meta_schema).alias("meta")) \
    .select("meta.*")

# Join the two streams on `guid`
joined_df = scanner_df.join(meta_df, on="guid", how="inner")

# Write to MinIO in Parquet format
query = joined_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://europeana-data/scanner-metadata/") \
    .option("checkpointLocation", "/tmp/checkpoints/scanner-metadata") \
    .start()

query.awaitTermination()
