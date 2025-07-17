FROM bitnami/spark:3.5.5

USER root
WORKDIR /app


RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 psycopg2-binary pandas

USER 1001


COPY spark-apps/curated-to-postgres/ .


CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  curated_to_postgres.py
