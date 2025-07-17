# works
# overwrite
# file that converts Europeana metadata to Delta format on MinIO.
# This script:
# reads Europeana metadata in JSON format from MinIO.
# Cleans the data by removing records with null or duplicate GUIDs.
# Writes the cleaned data in Delta format to MinIO.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# ---------------- SparkSession configured for MinIO + Delta ----------------
spark = SparkSession.builder \
    .appName("CleanseEuropeanaToDelta") \
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

# ---------------- Reading raw JSON Europeana ----------------
print("Reading raw JSON from MinIO...")
df = spark.read.json("s3a://heritage/raw/metadata/europeana_metadata/")
print(f"Number of records read: {df.count()}")

# ---------------- Data cleansing ----------------
df_clean = df \
    .filter(col("guid").isNotNull()) \
    .filter(col("image_url").isNotNull()) \
    .dropDuplicates(["guid"])
print(f"Record number after cleaning: {df_clean.count()}")
print("Writing in Delta format on MinIO...")

#  Tranforms also empty strings “” into null
fields_to_clean = [
    "title", "description", "timestamp_created", "provider","creator", "subject", "language", "type",
    "format", "rights", "dataProvider", "isShownAt", "edm_rights"
]

for field in fields_to_clean:
    df_clean = df_clean.withColumn(
        field,
        when(col(field) == "", lit(None)).otherwise(col(field))
    )

# ---------------- Writing in Delta format ----------------
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://heritage/cleansed/europeana/")
print("Job successfully completed")
spark.stop()
