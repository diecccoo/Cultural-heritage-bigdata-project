from pyspark.sql import SparkSession

# Crea la sessione Spark
spark = SparkSession.builder.appName("DeltaToPostgres").getOrCreate()

# Configurazione accesso MinIO (via s3a)
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.access.key", "minio")
hadoopConf.set("fs.s3a.secret.key", "minio123")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Lettura dati Delta Lake da MinIO
df = spark.read.format("delta").load("s3a://heritage/cleansed/europeana/")

# Scrittura su PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/heritage") \
    .option("dbtable", "europeana_items") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
