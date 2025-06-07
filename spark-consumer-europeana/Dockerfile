# (avvia spark-submit automaticamente)
FROM bitnami/spark:latest
# Installa Python e pip
USER root
RUN apt-get update && apt-get install -y python3-pip

# Copia requirements e installa dipendenze
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Imposta le variabili d'ambiente per Spark
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV SPARK_WORKER_CORES=1
ENV SPARK_WORKER_MEMORY=1G

USER 1001

# Comando di avvio
CMD [ "spark-submit", \
      "--master", "spark://spark-master:7077", \
      "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
      "--conf", "spark.executor.memory=1g", \
      "--conf", "spark.driver.memory=1g", \
      "/opt/spark-apps/ingestion/kafka_eu_md_to_minio.py" ]





# # Nessun COPY! Il codice viene montato da volume.

# # Comando di avvio: esegue spark-submit direttamente
# CMD [ "spark-submit", "/opt/spark-apps/ingestion/kafka_eu_md_to_minio.py" ]
