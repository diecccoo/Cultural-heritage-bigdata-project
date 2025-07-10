FROM bitnami/spark:3.5.5
ENV HOME=/opt/bitnami/spark

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Torna all'utente non-root
USER 1001

# Copia lo script nello container
COPY spark-apps/embeddings-exporter/ .

# Avvio dello script con i pacchetti necessari (solo Delta)
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  export_embeddings.py
