from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UGCtoPostgres").getOrCreate()

# Configurazione MinIO
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Lettura Delta Lake UGC
df = spark.read.format("delta").load("s3a://heritage/cleansed/user_generated/")

# Scrittura su PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/heritage") \
    .option("dbtable", "ugc_annotations") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
