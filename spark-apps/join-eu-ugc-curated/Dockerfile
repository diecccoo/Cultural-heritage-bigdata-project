FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Torna all'utente non-root
USER 1001

# Copia gli script del container join-eu-ugc-curated
COPY spark-apps/join-eu-ugc-curated/ .

# Avvio dello script con Delta Lake
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  join_deltatables_eu_ugc.py
