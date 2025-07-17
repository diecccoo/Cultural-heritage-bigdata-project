from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_json
from pyspark.sql.types import ArrayType, StringType
import sys
import psycopg2

def log(msg):
    print(f"[Curated→Postgres] {msg}")

spark = SparkSession.builder.appName("CuratedToPostgres").getOrCreate()

# Config MinIO
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

    df = df.withColumn("image_url", from_json("image_url", ArrayType(StringType())))
    df = df.withColumn("subject", from_json("subject", ArrayType(StringType())))


    # Seleziona TUTTI i 23 campi richiesti
    df_mapped = df.select(
        col("guid"),
        col("user_id"),
        col("tags"),
        col("comment"),
        to_timestamp("timestamp").alias("timestamp"),
        col("source"),
        col("creator"),
        col("description"),
        col("edm_rights"),
        col("format"),
        col("image_url"),
        col("language"),
        col("provider"),
        col("subject"),
        col("title"),
        col("type")
    )



    log("Complete schema ready for PostgreSQL")
    df_mapped.printSchema()
    log(" Example rows (first 5):")
    df_mapped.show(5, truncate=False)

    log("Total row count:")
    log(f"Total rows to write in PostgreSQL: {df_mapped.count()}")
    
    log("Real Spark Types:")
    print(df_mapped.dtypes)



    # ====== Scrivi su tabella STAGING ======
    df_mapped.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/heritage") \
        .option("dbtable", "join_metadata_staging") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    log("Writing completed in join_metadata_staging")

except Exception as e:
    log(f"Error during the process: {str(e)}")
    sys.exit(1)

try:
    log("performing refresh from STAGING to final table...")

    conn = psycopg2.connect(
        host="postgres",
        dbname="heritage",
        user="postgres",
        password="postgres"
    )
    conn.autocommit = False  #  Initiate the transaction
    cur = conn.cursor()
    log("Transaction initiated")

    # 1. Delete data from the final table
    cur.execute("DELETE FROM join_metadata_deduplicated;")
    log("DELETE completed")

    # 2. Insert the data from staging
    cur.execute("""
        INSERT INTO join_metadata_deduplicated (
            guid, user_id, tags, comment, timestamp,
            source, creator, description, edm_rights, format, image_url,
            language, provider, subject,
            title, type
        )
        SELECT 
            guid, user_id, tags, comment, timestamp,
            source, creator, description, edm_rights, format, image_url,
            language, provider, subject,
            title, type
        FROM join_metadata_staging;
    """)
    log("INSERT completed")

    # 3. Final commit 
    conn.commit()
    log("Final Commit Completed")

except Exception as e:
    log(f"Error during refresh: {e}")
    if 'conn' in locals():
        conn.rollback()
        log("ROLLBACK executed (transaction cancelled)")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    log("Connection PostgreSQL closed")
