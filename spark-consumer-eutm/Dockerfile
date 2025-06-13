# Dockerfile per Spark 3.5.0 Bitnami
FROM bitnami/spark:3.5.0

# Passa a root per installare Python
USER root

# Installa Python 3 e pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    apt-get clean

# Copia e installa librerie Python
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Imposta la cartella di configurazione Spark
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

# Torna all’utente non-root (Bitnami usa 1001)
USER 1001

# Comando di default: esegui lo script PySpark
CMD [ "spark-submit", "--master", "spark://spark-master:7077", "/opt/spark-apps/kafka_europeana_metadata_to_minio.py" ]
