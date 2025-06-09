# questo script:
# Salva i JSON originali (utile per backup/debug)
# Deduplica in base a guid
# Scrive in Parquet leggibile e performante per Spark SQL
# I due stream funzionano in parallelo
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType
import logging

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Schema dei metadati Europeana
schema = StructType() \
    .add("title", StringType()) \
    .add("guid", StringType()) \
    .add("image_url", StringType()) \
    .add("timestamp_created", StringType()) \
    .add("provider", StringType()) \
    .add("description", StringType()) \
    .add("creator", StringType()) \
    .add("subject", StringType()) \
    .add("language", StringType()) \
    .add("type", StringType()) \
    .add("format", StringType()) \
    .add("rights", StringType()) \
    .add("dataProvider", StringType()) \
    .add("isShownAt", StringType()) \
    .add("isShownBy", StringType()) \
    .add("edm_rights", StringType())


# Avvio SparkSession con configurazione MinIO
spark = SparkSession.builder \
    .appName("EuropeanaKafkaToMinIO") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
logging.info("Avvio consumer Spark per Europeana...")

# Lettura grezza da Kafka
# kafka.bootstrap.servers È una lista di broker iniziali, che Spark usa per collegarsi al cluster. 
# Kafka non ha bisogno di sapere tutti i broker, ma almeno uno valido per ottenere la lista completa dei broker dal controller e iniziare la connessione
# Produce colonne:
#   -key e value (binari)
#   -topic, partition, offset, timestamp, timestampType
# earliest significa che legge tutto dallo storico --> usare "latest" Se vuoi solo i nuovi messaggi dopo l'avvio della pipeline (es. in produzione)
# castra value da binario a stringa (per parsing JSON)

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9093,kafka3:9094") \
    .option("subscribe", "europeana_metadata") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str")



#  DEBUG: stampa ogni batch ricevuto (raw json)
def debug_batch(batch_df, batch_id):
    count = batch_df.count()
    logging.info(f"🔍 Batch {batch_id}: Ricevuti {count} messaggi da Kafka")
    if count > 0:
        batch_df.show(3, truncate=False)

debug_query = raw_df.writeStream \
    .foreachBatch(debug_batch) \
    .outputMode("append") \
    .start()

# Parsing JSON
parsed_df = raw_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Scrittura dei JSON originali su MinIO (raw)
def process_json_batch(batch_df, batch_id):
    count = batch_df.count()
    logging.info(f"💾 Batch JSON {batch_id}: Salvando {count} record in MinIO raw/")
    if count > 0:
        batch_df.show(3, truncate=False)

raw_json_query = parsed_df.writeStream \
    .foreachBatch(process_json_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/metadata_europeana_json") \
    .option("path", "s3a://heritage/raw/metadata/metadata_europeana/") \
    .outputMode("append") \
    .start()

#  Scrittura deduplicata in formato Parquet
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


# Aspetta che almeno uno stream termini (e logga se si interrompe)
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    logging.error(f"❌ Errore durante l'esecuzione di uno stream: {str(e)}")
    raise
