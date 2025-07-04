FROM bitnami/spark:3.5.5

USER root
WORKDIR /app

# Installa librerie Spark e Delta
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

USER 1001

# Copia lo script Spark
COPY spark-apps/curated-to-postgres/ .

# Avvia Spark con Delta e JDBC PostgreSQL
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  curated_to_postgres.py
