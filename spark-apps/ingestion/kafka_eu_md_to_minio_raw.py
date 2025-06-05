# Script per leggere i metadati Europeana da Kafka e salvarli in MinIO in raw/metadata/metadata_europeana/
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType


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
# guid è id europeana unico
# image_url è l'URL dell'immagine associata al metadato -->Per scaricare automaticamente le immagini da image_url a raw/images/, possiamo creare uno script batch Spark o uno script Python parallelo (es. con concurrent.futures).

# Spark Session con configurazione per MinIO
spark = SparkSession.builder \
    .appName("EuropeanaKafkaToMinIO") \
    .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.5.1.jar,/opt/spark/jars/hadoop-aws.jar,/opt/spark/jars/aws-java-sdk-bundle.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# print("Spark JARs:", spark.sparkContext._conf.get("spark.jars"))

# Lettura da Kafka topic europeana_metadata
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092,kafka2:9093,kafka3:9094") \
    .option("subscribe", "europeana_metadata") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str")

# Estrazione e parsing dei messaggi JSON
parsed_df = raw_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# vecchio:
# parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# Scrittura su console per DEBUG (visualizza JSON sul terminale)
query_console = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()



# Scrittura continua in MinIO (come file JSON) in metadata_europeana
# query = parsed_df.writeStream \
#     .format("json") \
#     .option("checkpointLocation", "/tmp/checkpoints/metadata_europeana") \
#     .option("path", "s3a://heritage/raw/metadata/metadata_europeana/") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()


# query.awaitTermination()
query_console.awaitTermination()
