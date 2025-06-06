# (avvia spark-submit automaticamente)
FROM bitnami/spark:latest

# Nessun COPY! Il codice viene montato da volume.
# Comando di avvio: esegue spark-submit direttamente
CMD [ "spark-submit", "/opt/spark-apps/ingestion/kafka_eu_md_to_minio.py" ]
