FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 numpy

# Torna all'utente non-root
USER 1001

# Copia lo script nello container
COPY spark-apps/deduplicator-to-cleansed/ .

# Avvio dello script Spark con pacchetti Delta
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  deduplicate_from_faiss_candidates.py
