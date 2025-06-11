# kafka_europeana_metadata_to_minio.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Recupera le variabili d'ambiente
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092,kafka2:9093")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "heritage")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "europeana_metadata")

print(f"DEBUG: Avvio SparkSession per topic '{KAFKA_TOPIC}' e bucket '{MINIO_BUCKET}' su MinIO endpoint '{MINIO_ENDPOINT}'")

# Inizializza Spark Session con le configurazioni per MinIO
spark = SparkSession.builder \
    .appName("KafkaToMinIO_SimpleTest") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("DEBUG: SparkSession creata con successo.")

# Leggi i messaggi dal topic Kafka
# Useremo 'value' come stringa, assumendo che i tuoi messaggi JSON siano qui
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

print(f"DEBUG: In attesa di messaggi dal topic Kafka '{KAFKA_TOPIC}'...")

# Seleziona solo il campo 'value' e lo converte in stringa
# Questo rappresenta il tuo JSON raw
raw_messages_df = kafka_df.selectExpr("CAST(value AS STRING) as message_content")

# Scrivi i messaggi raw in una cartella di test su MinIO
# Useremo il formato 'text' dove ogni riga è un messaggio
output_path = f"s3a://{MINIO_BUCKET}/simple_test_output/"
checkpoint_path = "/tmp/spark_checkpoint_simple_test/"

print(f"DEBUG: Inizio scrittura dati in '{output_path}'")

query = raw_messages_df.writeStream \
    .format("text") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(once=True) \
    .start()

print("DEBUG: Job Spark avviato. Attendo la sua terminazione...")

query.awaitTermination()

print(f"DEBUG: Job Spark terminato. Controlla la cartella '{output_path}' nel tuo bucket MinIO.")
print("DEBUG: Se vedi file, il collegamento è funzionante!")

spark.stop()
print("DEBUG: SparkSession chiusa.")