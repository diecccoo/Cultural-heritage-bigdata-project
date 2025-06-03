# comando: 
# docker exec -it spark-master spark-submit /opt/spark-apps/ingestion/kafka_annotations_to_minio_raw.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType

#  Schema JSON dei messaggi inviati da MQTT (via Kafka)
schema = StructType() \
    .add("object_id", StringType()) \
    .add("user_id", StringType()) \
    .add("tags", ArrayType(StringType())) \
    .add("comment", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType())

#  Crea la SparkSession configurata per MinIO (s3a)
spark = SparkSession.builder \
    .appName("KafkaToMinIOAnnotations") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#  Lettura in streaming dal topic Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "heritage_annotations") \
    .option("startingOffsets", "latest") \
    .load()

#  Parsing del campo `value` come JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

#  Scrittura dei messaggi su MinIO in formato JSON
query = df_parsed.writeStream \
    .format("json") \
    .option("path", "s3a://heritage/raw/metadata/") \
    .option("checkpointLocation", "/tmp/checkpoints/annotations-ingestion") \
    .outputMode("append") \
    .start()

query.awaitTermination()