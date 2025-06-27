# kafka_europeana_metadata_to_minio.py
#script di matteo che prende i dati da kafka e li scrive su delta lake con spark

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import traceback
import time
from pyspark.sql.types import StructType, StringType, ArrayType


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

spark = None
query = None

try:
    # Inizializza SparkSession
    spark = SparkSession.builder \
        .appName("EuropeanaKafkaToMinIO") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession inizializzata")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "europeana_metadata") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \


    print("📥 Connessione a Kafka stabilita, topic: europeana_metadata")
# cambiare questa riga per 3 brokers!!! (kafka3)
# .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9093,kafka3:9094") \
# .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9093")



# Parsing JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")
    
    print("🧠 Schema applicato, avvio del writeStream...")


    # Scrittura su Delta Lake (MinIO)
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://heritage/parquet/metadata_europeana/_checkpoints") \
        .start("s3a://heritage/parquet/metadata_europeana/")

    query.awaitTermination()

except Exception as e:
    print("❌ Errore durante l'esecuzione del consumer:")
    traceback.print_exc()


finally:
    print("🛑 Shutdown del job Spark")
    if query:
        try:
            query.stop()
        except:
            pass
    if spark:
        try:
            spark.stop()
        except:
            pass
  