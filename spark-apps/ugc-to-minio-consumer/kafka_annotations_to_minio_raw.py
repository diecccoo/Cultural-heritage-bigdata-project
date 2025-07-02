# Questo script:
# Legge messaggi JSON dal topic Kafka chiamato user_annotations
# Fa parsing del campo value come JSON con uno schema definito
# Scrive i dati (in formato JSON) in MinIO in heritage/raw/metadata_ugc/


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, ArrayType

# Schema dei messaggi Kafka (user annotations)
schema = StructType() \
    .add("object_id", StringType()) \
    .add("user_id", StringType()) \
    .add("tags", ArrayType(StringType())) \
    .add("comment", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType())

# SparkSession configurata per MinIO con Delta support
spark = SparkSession.builder \
    .appName("KafkaToMinIOAnnotationsDelta") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lettura dallo stream Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_annotations") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing del campo `value` come JSON e aggiunta di `ingestion_time`
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp())

# Scrittura su Delta Lake in MinIO
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://heritage/raw/metadata/user_generated_content/_checkpoints") \
    .start("s3a://heritage/raw/metadata/user_generated_content/")

query.awaitTermination()
