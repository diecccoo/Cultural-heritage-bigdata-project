FROM bitnami/spark:3.5.5


USER root
WORKDIR /app

# Install Spark + Delta +  S3
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# back to  non-root user
USER 1001


COPY spark-apps/eu-to-cleansed/ .

CMD ["python", "scheduler.py"]
