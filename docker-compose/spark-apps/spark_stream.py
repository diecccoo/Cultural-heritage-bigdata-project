from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("ScannerConsumer").getOrCreate()

schema = StructType() \
    .add("scanId", StringType()) \
    .add("uri", StringType()) \
    .add("timestamp", StringType()) \
    .add("mime", StringType())

df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "new-scans")
    .load()
)

parsed = (df.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

query = (parsed.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()
