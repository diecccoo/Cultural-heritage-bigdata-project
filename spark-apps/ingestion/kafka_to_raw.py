from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType

# Schema dei messaggi
schema = StructType() \
    .add("user_id", StringType()) \
    .add("object_id", StringType()) \
    .add("tags", ArrayType(StringType())) \
    .add("comment", StringType()) \
    .add("timestamp", StringType())

# Spark session configurata per MinIO
spark = SparkSession.builder \
    .appName("KafkaToMinIOIngestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Legge i messaggi da Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "heritage_annotations") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Scrive in MinIO in formato JSON
query = df_parsed.writeStream \
    .format("json") \
    .option("path", "s3a://heritage/raw/metadata/") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka-to-minio") \
    .outputMode("append") \
    .start()

query.awaitTermination()
