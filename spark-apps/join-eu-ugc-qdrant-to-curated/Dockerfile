FROM bitnami/spark:3.5.5

# root for installing dependencies
USER root
WORKDIR /app

# install library dependencies
COPY spark-apps/join-eu-ugc-qdrant-to-curated/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


USER 1001

# Copy script container join-eu-ugc-curated
COPY spark-apps/join-eu-ugc-qdrant-to-curated/ .


CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  join_eu_ugc_qdrant_merge.py
