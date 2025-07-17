FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa Spark + Delta + supporto S3
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Torna a utente non-root
USER 1001

# Copia gli script nel container
COPY query/ .

# Lancia lo scheduler
CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "io.delta:delta-spark_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.2", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", \
     "--conf", "spark.hadoop.fs.s3a.access.key=minio", \
     "--conf", "spark.hadoop.fs.s3a.secret.key=minio123", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", \
     "minio_query.py"]