from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
import sys

def log(msg):
    print(f"[EU→Postgres] {msg}")

log("Inizio script Delta Europeana")

spark = SparkSession.builder.appName("DeltaToPostgres").getOrCreate()

# Configurazione MinIO
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.access.key", "minio")
hadoopConf.set("fs.s3a.secret.key", "minio123")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

log("MinIO configurato")

try:
    # Carica dati da Delta
    df = spark.read.format("delta").load("s3a://heritage/cleansed/europeana/")
    log("Dati Delta caricati")

    # Prepara schema per PostgreSQL
    selected = df.select(
        "guid", "title", "creator", "image_url",
        "timestamp_created", "provider", "description",
        "subject", "language", "type", "format",
        "rights", "dataProvider", "isShownAt", "isShownBy", "edm_rights"
    )

    # Aggiungi campo raw_json come JSONB (solo per completezza)
    df_final = selected.withColumn("raw_json", to_json(struct(*selected.columns)))

    df_final.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/heritage") \
        .option("dbtable", "europeana_items") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    log("✅ Scrittura completata su europeana_items")

except Exception as e:
    log(f"❌ Errore: {str(e)}")
    sys.exit(1)
