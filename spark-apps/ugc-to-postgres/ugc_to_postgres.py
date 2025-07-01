from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys

def log(msg):
    print(f"[UGC→Postgres] {msg}")

log("Inizio script Spark")

# Crea la sessione Spark
spark = SparkSession.builder.appName("UGCtoPostgres").getOrCreate()

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
    # Legge Delta Lake UGC
    df = spark.read.format("delta").load("s3a://heritage/cleansed/user_generated/")
    log("Delta UGC caricato correttamente")

    # Mappa i campi per PostgreSQL
    df_mapped = df.select(
        col("object_id").alias("object_guid"),
        col("user_id"),
        col("comment"),
        col("tags"),
        to_timestamp(col("timestamp")).alias("ugc_timestamp"),
        col("location")
    )

    log("Schema mappato per PostgreSQL")
    df_mapped.printSchema()

    # Scrive su PostgreSQL
    df_mapped.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/heritage") \
        .option("dbtable", "ugc_annotations") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    log("✅ Scrittura completata con successo in PostgreSQL")

except Exception as e:
    log(f"❌ Errore durante il processo: {str(e)}")
    sys.exit(1)
