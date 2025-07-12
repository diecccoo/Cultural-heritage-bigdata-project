FROM bitnami/spark:3.5.5

# serve per installare Python libs
USER root
WORKDIR /app


# Installa le dipendenze Python: Spark + Delta + Kafka client
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 kafka-python

# torna all'utente non-root
USER 1001

COPY kafka-producers/annotation-producer/ .

# Avvio con spark-submit per avere a disposizione i connettori Delta e S3A
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  annotation_producer.py

