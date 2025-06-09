# Base Spark image
FROM bitnami/spark:latest

# Installa Python e pip
USER root
RUN apt-get update && apt-get install -y python3-pip

# Copia solo requirements.txt per sfruttare la cache
# requirements.txt si trova nella stessa directory del Dockerfile
COPY spark-consumer-europeana/requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# --- NUOVE RIGHE IMPORTANTI PER I JAR ---
# Crea la directory dove verranno copiati i JAR all'interno del container
RUN mkdir -p /opt/spark/jars/
# Copia tutti i JAR dalla tua directory locale 'config/spark/jars'
# Questo percorso è relativo alla radice del contesto di build (tuo_progetto/)
COPY config/spark/jars/ /opt/spark/jars/


# Torna all'utente non-root (bitnami/spark usa 1001)
USER 1001

# Copia lo script dell'applicazione PySpark
# Questo percorso è relativo alla radice del contesto di build (tuo_progetto/)
COPY spark-apps/ /opt/spark-apps/

# --- COMANDO CMD (COME PRIMA) ---
CMD [ "spark-submit", \
      "--master", "spark://spark-master:7077", \
      "--conf", "spark.executor.memory=1g", \
      "--conf", "spark.driver.memory=1g", \
      "--conf", "spark.sql.extensions=org.apache.spark.sql.delta.DeltaSparkSessionExtension", \
      "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
      "/opt/spark-apps/ingestion/kafka_eu_md_to_minio.py" ]



#  WILDCARD * NON FUNZIONA per jars
# "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-4.0.0.jar,/opt/spark/jars/kafka-clients-3.6.0.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-4.0.0.jar,/opt/spark/jars/hadoop-aws.jar,/opt/spark/jars/jackson-core-2.14.2.jar,/opt/spark/jars/jackson-annotations-2.14.2.jar,/opt/spark/jars/jackson-databind-2.14.2.jar",\
