FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
COPY spark-apps/join-eu-ugc-qdrant-to-curated/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Torna all'utente non-root
USER 1001

# Copia gli script del container join-eu-ugc-curated
COPY spark-apps/join-eu-ugc-qdrant-to-curated/ .

# Avvio dello script con Delta Lake
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  join_eu_ugc_qdrant_comments.py
