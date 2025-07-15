#file che converte i metadati Europeana in formato Delta su MinIO
# Questo script:
# Legge i metadati Europeana in formato JSON da MinIO
# Pulisce i dati rimuovendo record con GUID nulli o duplicati
# Scrive i dati puliti in formato Delta su MinIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# ---------------- SparkSession configurata per MinIO + Delta ----------------
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

# ---------------- Lettura dei JSON grezzi Europeana ----------------
print("📥 Lettura JSON grezzi da MinIO...")
df = spark.read.json("s3a://heritage/raw/metadata/europeana_metadata/")
print(f"📊 Numero record letti: {df.count()}")

# ---------------- Data cleansing ----------------
df_clean = df \
    .filter(col("guid").isNotNull()) \
    .filter(col("image_url").isNotNull()) \
    .dropDuplicates(["guid"])
print(f"🧹 Numero record dopo cleaning: {df_clean.count()}")
print("💾 Scrittura in formato Delta su MinIO...")

#  trasforma anche le stringhe vuote "" in null.
fields_to_clean = [
    "title", "description", "timestamp_created", "provider","creator", "subject", "language", "type",
    "format", "rights", "dataProvider", "isShownAt", "edm_rights"
]

for field in fields_to_clean:
    df_clean = df_clean.withColumn(
        field,
        when(col(field) == "", lit(None)).otherwise(col(field))
    )

# ---------------- Scrittura in formato Delta ----------------
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://heritage/cleansed/europeana/")
print("✅ Job completato con successo.")
spark.stop()
