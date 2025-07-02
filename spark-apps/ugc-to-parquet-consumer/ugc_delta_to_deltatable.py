from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------- SparkSession configurata per MinIO + Delta ----------------
spark = SparkSession.builder \
    .appName("CleanseUGCtoDelta") \
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

# ---------------- Lettura Delta raw ----------------
df = spark.read.format("delta").option("ignoreChanges", "false").load("s3a://heritage/raw/metadata/user_generated_content/")

# ---------------- Data cleansing ----------------
df_clean = df \
    .filter(col("object_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter((col("tags").isNotNull()) & (col("tags").getItem(0).isNotNull()) & (col("tags").getItem(0).rlike(r"\S"))) \
    .dropDuplicates(["object_id", "user_id", "comment"])

# ---------------- Debug: count + anteprima clean ----------------
print("🧼 fatto cleaning")
df_clean.show(3, truncate=False)

# ---------------- Scrittura in Delta ----------------
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://heritage/cleansed/user_generated/")

print("✅ Job completato con successo.")
spark.stop()
