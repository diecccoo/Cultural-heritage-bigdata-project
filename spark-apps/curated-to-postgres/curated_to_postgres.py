from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.functions import element_at
import sys

def log(msg):
    print(f"[Curated→Postgres] {msg}")

log("Inizio script Spark")

# Crea la sessione Spark
spark = SparkSession.builder.appName("CuratedToPostgres").getOrCreate()

log("Sessione Spark creata")

# Configura MinIO
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.access.key", "minio")
hadoopConf.set("fs.s3a.secret.key", "minio123")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

log("Configurazione MinIO completata")

try:
    # Legge la Delta Table joinata
    df = spark.read.format("delta").load("s3a://heritage/curated/join_metadata/")
    log("Delta join_metadata caricata correttamente")


    # Seleziona e mappa i campi desiderati (immagine e link convertiti da array a stringa)
    df_mapped = df.select(
    col("guid").alias("id_object"),
    col("user_id"),
    col("tags"),
    col("comment"),
    to_timestamp(col("timestamp")).alias("annotation_timestamp"),
    col("description"),
    col("image_url"),
    col("title"),
    col("creator")
)


    log("Schema pronto per PostgreSQL")
    df_mapped.printSchema()

    # Scrive su PostgreSQL
    df_mapped.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/heritage") \
        .option("dbtable", "join_metadata") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    log("✅ Scrittura completata con successo in PostgreSQL")

except Exception as e:
    log(f"❌ Errore durante il processo: {str(e)}")
    sys.exit(1)
