from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import traceback
from pyspark.sql.types import StructType, StringType, ArrayType
import json
import uuid
import re

def sanitize_filename(name):
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', name)

# Schema of metadata Europeana
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
    .add("edm_rights", StringType())

spark = None
query = None

try:
    # Initialize SparkSession
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
    print("SparkSession initialized")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "europeana_metadata") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \


    print("Connection to Kafka established, topic: europeana_metadata")


# Parsing JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

    print("Schema applied, starting writeStream...")


    # Write to Delta Lake (MinIO)
    def write_each_row_as_json(batch_df, batch_id):
        print(f"Writing batch {batch_id}...")
        
        s3_base = "s3a://heritage/raw/metadata/europeana_metadata/"
        rows = batch_df.collect()

        for row in rows:
            try:
                # Use the guid or create a unique name
                guid = row["guid"] or f"no-guid-{uuid.uuid4()}"
                safe_guid = sanitize_filename(guid)
                file_name = f"{safe_guid}.json"
                file_path = s3_base + file_name


                # Convert the row to pure JSON (dict -> str)
                json_data = json.dumps(row.asDict())

                # Write using Hadoop FileSystem API di Spark
                hadoop_conf = spark._jsc.hadoopConfiguration()
                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(s3_base),
                    hadoop_conf
                )

                output_stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(file_path))
                output_stream.write(bytearray(json_data, "utf-8"))
                output_stream.close()

                # print(f"Written: {file_name}")
            except Exception as e:
                print(f"Error writing {row}: {e}")
        print(f"Batch {batch_id} completed")

    query = parsed_df.writeStream \
    .foreachBatch(write_each_row_as_json) \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://heritage/raw/metadata/europeana_metadata/_checkpoints_eachjson") \
    .start()



    query.awaitTermination()

except Exception as e:
    print("Error occurred while running the consumer:")
    traceback.print_exc()


finally:
    print("Shutdown del job Spark")
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
  