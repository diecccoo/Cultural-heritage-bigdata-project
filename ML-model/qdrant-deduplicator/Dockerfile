FROM python:3.10-slim

USER root
WORKDIR /app

# requirements 
RUN pip install --no-cache-dir \
    qdrant-client==1.6.4 \
    tqdm \
    scikit-learn \
    numpy 

# Copy script Python
COPY ML-model/qdrant-deduplicator/ .


CMD ["python", "deduplicate_from_qdrant.py"]
