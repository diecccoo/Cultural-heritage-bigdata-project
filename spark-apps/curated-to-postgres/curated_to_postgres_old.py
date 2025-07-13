from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_json
from pyspark.sql.types import ArrayType, StringType
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
    df = spark.read.format("delta").load("s3a://heritage/curated/join_metadata_deduplicated/")
    log("Delta join_metadata_deduplicated caricata correttamente")

    df = df.withColumn("dataProvider", from_json("dataProvider", ArrayType(StringType())))
    df = df.withColumn("image_url", from_json("image_url", ArrayType(StringType())))
    df = df.withColumn("isShownBy", from_json("isShownBy", ArrayType(StringType())))
    df = df.withColumn("subject", from_json("subject", ArrayType(StringType())))


    # Seleziona TUTTI i 23 campi richiesti
    df_mapped = df.select(
        col("guid").alias("id_object"),
        col("user_id"),
        col("tags"),
        col("comment"),
        to_timestamp("timestamp").alias("timestamp"),
        col("location"),
        to_timestamp("ingestion_time").alias("ingestion_time_ugc"),
        col("source"),
        col("creator"),
        col("dataProvider"),
        col("description"),
        col("edm_rights"),
        col("format"),
        col("image_url"),
        col("isShownBy"),
        col("language"),
        col("provider"),
        col("rights"),
        col("subject"),
        to_timestamp("timestamp_created").alias("timestamp_created_europeana"),
        col("title"),
        col("type"),
        to_timestamp("joined_at").alias("joined_at")
    )



    log("Schema completo pronto per PostgreSQL")
    df_mapped.printSchema()
    log("🔍 Esempio righe (primi 5):")
    df_mapped.show(5, truncate=False)

    log("🔢 Conteggio righe totali:")
    log(f"Totale righe da scrivere in PostgreSQL: {df_mapped.count()}")
    
    log("📋 Tipi Spark reali:")
    print(df_mapped.dtypes)



    # Scrive su PostgreSQL
    df_mapped.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/heritage") \
        .option("dbtable", "join_metadata_deduplicated") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    log("✅ Scrittura completata con successo in PostgreSQL")

except Exception as e:
    log(f"❌ Errore durante il processo: {str(e)}")
    sys.exit(1)
