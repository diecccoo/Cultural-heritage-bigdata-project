FROM python:3.10-slim

USER root
WORKDIR /app

# Copia i requirements e installa
RUN pip install --no-cache-dir \
    qdrant-client==1.6.4 \
    tqdm \
    scikit-learn \
    numpy 

# Copia lo script Python
COPY ML-model/qdrant-deduplicator/ .

# Esegui lo script (loop ogni 5 min è già nel file .py)
CMD ["python", "deduplicate_from_qdrant.py"]
