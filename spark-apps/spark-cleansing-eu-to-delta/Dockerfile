FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa Spark + Delta + supporto S3
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Torna a utente non-root
USER 1001

# Copia gli script nel container
COPY spark-apps/spark-cleansing-eu-to-delta/ .

# Lancia lo scheduler
CMD ["python", "scheduler.py"]
