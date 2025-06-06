FROM python:3.9-slim

WORKDIR /app

# Copia solo file necessari
COPY publish_mqtt_annotations_from_minio.py .

# Installa le dipendenze necessarie
RUN pip install --no-cache-dir minio paho-mqtt

# Comando di default all'avvio del container
CMD ["python", "publish_mqtt_annotations_from_minio.py"]
