FROM bitnami/spark:3.5.5

USER root
WORKDIR /app

RUN pip install --no-cache-dir \
    pyspark==3.5.5 \
    delta-spark==3.3.1 \
    torch \
    torchvision \
    transformers \
    pillow \
    requests \
    boto3 \
    pandas \
    numpy

ENV TRANSFORMERS_CACHE=/root/.cache/huggingface/transformers
ENV HF_HOME=/root/.cache/huggingface



COPY spark-apps/embeddings-extractor/ .

CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  extracting_embeddings.py
