FROM bitnami/spark:3.5.5

# Uses root for installations
USER root
WORKDIR /app

# Installs only Spark + Delta + standard Python
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1

# Switch back to non-root user
USER 1001

# Copy scripts into the container
COPY spark-apps/ugc-to-cleansed/ .

# Start the scheduler (which runs spark-submit every N seconds)
CMD ["python", "scheduler.py"]



