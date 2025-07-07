FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
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

# Crea la directory di cache e assicura i permessi
RUN mkdir -p /tmp/hf-cache && chmod -R 777 /tmp/hf-cache

ENV HF_HOME=/tmp/hf-cache

# Imposta la variabile di ambiente per la cache HuggingFace
ENV TRANSFORMERS_CACHE=/tmp/hf-cache




# Torna all'utente non-root
USER 1001

# Copia gli script del container embeddings-extractor
COPY spark-apps/embeddings-extractor/ .

# Avvio dello script con Delta Lake (usa PySpark locale per batch CPU)
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  extracting_embeddings.py
