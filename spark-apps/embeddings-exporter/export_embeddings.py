from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------
# SparkSession configurata per Delta Lake + MinIO
# ---------------------------------------------
spark = SparkSession.builder \
    .appName("Export Embeddings") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ---------------------------------------------
# Percorso della DeltaTable degli embeddings
# ---------------------------------------------
delta_path = "s3a://heritage/cleansed/embeddings/"

# ---------------------------------------------
# Lettura dei dati e filtro dei record validi
# ---------------------------------------------
df_light = spark.read.format("delta").load(delta_path).select("id_object", "embedding_status")

# Step 2: filtra solo i record con status "OK"
valid_ids = df_light.filter(col("embedding_status") == "OK")

# Step 3: ricarica l’intero dataset
df_full = spark.read.format("delta") \
    .load("s3a://heritage/cleansed/embeddings")

# Step 4: filtra solo i record validi via join
df_embeddings = df_full.join(valid_ids.select("id_object"), on="id_object", how="inner")

# ---------------------------------------------
# Selezione colonne da esportare
# ---------------------------------------------
export_df = df_embeddings.select("id_object", "embedding_image", "embedding_text")

# ---------------------------------------------
# Scrittura del file Parquet in volume condiviso
# ---------------------------------------------
export_df.write.mode("overwrite").parquet("/shared-data/embeddings.parquet")

print("✅ Esportazione completata con successo.")
spark.stop()
