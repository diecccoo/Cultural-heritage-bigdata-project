from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, ArrayType

# ---------------- Schema dei JSON raw ----------------
schema = StructType() \
    .add("object_id", StringType()) \
    .add("user_id", StringType()) \
    .add("tags", ArrayType(StringType())) \
    .add("comment", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("ingestion_time", StringType())

# ---------------- SparkSession configurata per MinIO ----------------
spark = SparkSession.builder \
    .appName("CleanseUGCtoParquet") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------- Lettura dei JSON raw ----------------
df = spark.read.schema(schema).json("s3a://heritage/raw/metadata/metadata_ugc/")

# ---------------- Data cleansing ----------------
df_clean = df \
    .filter(col("object_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter((col("tags").isNotNull()) & (col("tags").getItem(0).isNotNull()) & (col("tags").getItem(0).rlike(r"\S"))) \
    .dropDuplicates(["object_id", "user_id"])  # Rimozione duplicati logici

# ---------------- Scrittura in formato Parquet ----------------
df_clean.write \
    .mode("overwrite") \
    .parquet("s3a://heritage/cleansed/user_generated/")

spark.stop()
