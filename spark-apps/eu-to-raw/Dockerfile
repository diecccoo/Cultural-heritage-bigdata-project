FROM bitnami/spark:3.5.5

# Uses root for installations
USER root
WORKDIR /app

# Installs Python libraries
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Switch back to non-root user
USER 1001

# Copy scripts into the container
COPY spark-apps/eu-to-raw/ .

# Start the script with the necessary packages: Delta + Kafka
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  metadata_eu_to_raw.py