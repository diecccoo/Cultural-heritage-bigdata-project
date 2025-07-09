from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, array
from pyspark.sql.types import DoubleType
import numpy as np

# ----------------------------------------
# Cosine similarity UDF
# ----------------------------------------
def cosine_sim(a, b):
    # Convert PySpark arrays to numpy for computation
    a = np.array(a)
    b = np.array(b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return float("nan")
    return float(np.dot(a, b) / (norm_a * norm_b))

cosine_udf = udf(cosine_sim, DoubleType())

# ----------------------------------------
# SparkSession con Delta + S3A
# ----------------------------------------
spark = SparkSession.builder \
    .appName("Deduplicate From Faiss") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ----------------------------------------
# Percorsi
# ----------------------------------------
delta_path = "s3a://heritage/cleansed/embeddings/"
candidates_path = "/shared-data/faiss_output_topk.parquet"
output_path = "s3a://heritage/cleansed/deduplicated/"

# ----------------------------------------
# Caricamento embeddings + candidati
# ----------------------------------------
print("📥 Leggo DeltaTable embeddings...")
embeddings_df = spark.read.format("delta").load(delta_path) \
    .filter((col("embedding_status") == "OK") & col("embedding_image").isNotNull()) \
    .select("id_object", "embedding_image", "embedding_text")

print("📥 Leggo coppie da Faiss...")
faiss_df = spark.read.parquet(candidates_path)

# ----------------------------------------
# Join doppio per ottenere i vettori A e B
# ----------------------------------------
joined = faiss_df \
    .join(embeddings_df.withColumnRenamed("id_object", "id_a_ref").withColumnRenamed("embedding_image", "vec_a"), faiss_df.id_a == col("id_a_ref")) \
    .join(embeddings_df.withColumnRenamed("id_object", "id_b_ref").withColumnRenamed("embedding_image", "vec_b"), faiss_df.id_b == col("id_b_ref"))

# ----------------------------------------
# Calcolo cosine similarity esatta
# ----------------------------------------
joined = joined.withColumn("cosine_sim", cosine_udf(col("vec_a"), col("vec_b")))

# ----------------------------------------
# Filtra coppie simili
# ----------------------------------------
similar = joined.filter(col("cosine_sim") >= lit(0.98))

# ----------------------------------------
# Costruisce mapping duplicati → originali (min(id_a, id_b))
# ----------------------------------------
pairs = similar.selectExpr("id_a", "id_b") \
    .withColumn("group_id", col("id_a")) \
    .withColumn("dedup_id", lit(None)) \
    .withColumn("key", array("id_a", "id_b")) \
    .withColumn("dedup_id", array("id_a", "id_b")) \
    .withColumn("dedup_id", col("dedup_id")[0])  # usa il primo come rappresentante

# Normalizza: id_a, id_b → min(id_a, id_b)
from pyspark.sql.functions import least
pairs = pairs.withColumn("dedup_id", least("id_a", "id_b"))

# ----------------------------------------
# Costruisce tabella deduplicata
# ----------------------------------------
# unisci con embeddings originali → per ogni gruppo tieni solo 1 oggetto
all_ids = pairs.select("id_a").union(pairs.select("id_b")).distinct()
to_keep = all_ids.join(pairs, (all_ids.id_a == pairs.id_a) | (all_ids.id_a == pairs.id_b), how="left") \
                 .withColumn("representative", col("dedup_id")) \
                 .groupBy("representative").agg({"id_a": "min"}) \
                 .withColumnRenamed("min(id_a)", "id_object")

# Prendi solo oggetti unici
final_df = to_keep.join(embeddings_df, on="id_object", how="inner")

# ----------------------------------------
# Salvataggio DeltaTable finale
# ----------------------------------------
print("💾 Scrivo tabella deduplicata in:", output_path)
final_df.write.format("delta").mode("overwrite").save(output_path)

print("✅ Deduplicazione completata.")
spark.stop()
