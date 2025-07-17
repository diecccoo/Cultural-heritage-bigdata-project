FROM bitnami/spark:3.5.5

# is needed to install Python libs
USER root
WORKDIR /app


RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 kafka-python

# back to non-root user
USER 1001

COPY kafka-producers/annotation-producer/ .

CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  annotation_producer.py

