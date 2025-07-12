FROM python:3.10-slim

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python necessarie
RUN pip install --no-cache-dir faiss-cpu pandas pyarrow

# Torna all'utente non-root
USER 1001

# Copia lo script nello container
COPY faiss_knn.py .

# Avvio dello script Faiss
CMD ["python", "faiss_knn.py"]
