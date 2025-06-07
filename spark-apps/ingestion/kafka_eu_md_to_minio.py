# questo script:
# Salva i JSON originali (utile per backup/debug)
# Deduplica in base a guid
# Scrive in Parquet leggibile e performante per Spark SQL
# I due stream funzionano in parallelo

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType
import logging

# Configura logging più dettagliato
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definizione schema dei metadati Europeana
schema = StructType() \
    .add("title", StringType()) \
    .add("guid", StringType()) \
    .add("image_url", ArrayType(StringType())) \
    .add("timestamp_created", StringType()) \
    .add("query", StringType()) \
    .add("description", StringType()) \
    .add("creator", StringType()) \
    .add("subject", StringType()) \
    .add("language", StringType()) \
    .add("type", StringType()) \
    .add("format", StringType()) \
    .add("rights", StringType()) \
    .add("provider", StringType()) \
    .add("dataProvider", StringType()) \
    .add("isShownAt", StringType()) \
    .add("isShownBy", StringType()) \
    .add("edm_rights", StringType())

# Avvio SparkSession
spark = SparkSession.builder \
    .appName("EuropeanaKafkaToMinIO") \
    .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.5.1.jar,/opt/spark/jars/hadoop-aws.jar,/opt/spark/jars/aws-java-sdk-bundle.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Imposta log level per Spark
spark.sparkContext.setLogLevel("INFO")

logging.info("Avvio consumer Spark per Europeana...")

# Lettura da Kafka topic europeana_metadata
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "europeana_metadata") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str")
# .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9093,kafka3:9094") \

# Debug: stampa i dati grezzi
def debug_batch(batch_df, batch_id):
    count = batch_df.count()
    logging.info(f"🔍 Batch {batch_id}: Ricevuti {count} messaggi da Kafka")
    if count > 0:
        logging.info("📝 Esempio dei dati ricevuti:")
        batch_df.show(3, truncate=False)

raw_df.writeStream \
    .foreachBatch(debug_batch) \
    .outputMode("append") \
    .start()

# Parsing JSON in DataFrame con debug
parsed_df = raw_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Scrittura JSON grezzi in raw/ con debug
def process_json_batch(batch_df, batch_id):
    count = batch_df.count()
    logging.info(f"💾 Batch JSON {batch_id}: Salvando {count} record in MinIO raw/")
    if count > 0:
        logging.info("📝 Esempio dei dati parsati:")
        batch_df.show(3, truncate=False)

raw_json_query = parsed_df.writeStream \
    .foreachBatch(process_json_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/metadata_europeana_json") \
    .option("path", "s3a://heritage/raw/metadata/metadata_europeana/") \
    .outputMode("append") \
    .start()

# Scrittura Parquet deduplicato in parquet/ con debug
def process_parquet_batch(batch_df, batch_id):
    logging.info(f"📦 Processing batch {batch_id} for Parquet...")
    count = batch_df.count()
    logging.info(f"Found {count} records before deduplication")
    
    deduplicated = batch_df.dropDuplicates(["guid"])
    dedup_count = deduplicated.count()
    logging.info(f"Saving {dedup_count} records after deduplication")
    
    if dedup_count > 0:
        deduplicated.write \
            .mode("append") \
            .parquet("s3a://heritage/parquet/metadata_europeana")
        logging.info("✅ Batch successfully written to Parquet")

parquet_query = parsed_df.writeStream \
    .foreachBatch(process_parquet_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/metadata_europeana_parquet") \
    .outputMode("append") \
    .start()

logging.info("🚀 Tutti gli stream sono stati avviati. In attesa di dati...")

# Attendi terminazione con gestione errori
try:
    raw_json_query.awaitTermination()
    parquet_query.awaitTermination()
except Exception as e:
    logging.error(f"❌ Errore durante l'esecuzione: {str(e)}")
    raise
