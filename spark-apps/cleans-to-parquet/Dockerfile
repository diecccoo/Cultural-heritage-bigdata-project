FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa solo Spark + Delta + Python standard
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Torna all'utente non-root
USER 1001

# Copia gli script nel container
COPY spark-apps/cleans-to-parquet/ .

# Lancia lo scheduler (che ogni N secondi esegue spark-submit)
CMD ["python", "scheduler.py"]





#vecchio dockerfile

# FROM bitnami/spark:3.5.5

# # Usa root per installazioni
# USER root
# WORKDIR /app

# # Installa solo Spark + Delta (niente Kafka per questo job)
# RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# # Torna all'utente non-root
# USER 1001

# # Copia gli script nel container
# COPY cleans-to-parquet/ .

# # Avvio dello script Spark (solo Delta, niente Kafka)
# CMD spark-submit \
#   --master local[*] \
#   --packages io.delta:delta-spark_2.12:3.3.1 \
#   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#   ugc_json_to_parquet.py
