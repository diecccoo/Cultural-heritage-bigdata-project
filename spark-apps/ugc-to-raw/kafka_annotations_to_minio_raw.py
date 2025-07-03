# Questo script:
# Legge messaggi JSON dal topic Kafka chiamato user_annotations
# Scrive i dati (in formato JSON) in MinIO nel layer raw
# Partiziona per dt=YYYY-MM-DD, salva 1 file per batch, ogni 30 secondi

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, lit

print("[INFO] Inizializzazione SparkSession con accesso MinIO...")
spark = SparkSession.builder \
    .appName("KafkaToMinIO_RAW_JSON") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
print("[INFO] SparkSession creata.")

print("[INFO] Connessione a Kafka (topic: user_annotations)...")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_annotations") \
    .option("startingOffsets", "latest") \
    .load()

print("[INFO] Parsing e aggiunta metadati...")
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as value") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("kafka:user_annotations")) \
    .withColumn("dt", current_date())

print("[INFO] Avvio scrittura JSON su MinIO (coalesce(1), batch ogni 30s)...")
query = df_parsed.coalesce(1).writeStream \
    .format("json") \
    .option("path", "s3a://heritage/raw/metadata/user_generated_content/") \
    .option("checkpointLocation", "s3a://heritage/raw/checkpoints/user_annotations_json/") \
    .partitionBy("dt") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("[INFO] Scrittura avviata. In attesa di eventi...")
query.awaitTermination()
