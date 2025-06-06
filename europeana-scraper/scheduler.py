# ogni 6 ore Questo script:
# legge le keyword da queries.txt 
# Controlla da dove deve ripartire (offset.txt)
# Seleziona solo 2 query
# per ogni query, fa una chiamata all’API Europeana
# estrae tot item per query (puoi modificare rows=..)
# costruisce un dizionario JSON con i metadati (titolo, immagine, GUID…)
# invia ogni JSON su Kafka → topic: europeana_metadata
# Aggiorna offset.txt con la nuova posizione
# Aspetta 6 ore

# nuovo:
import time
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

while True:
    logging.info("Starting europeana_ingest_batch.py...")
    try:
        subprocess.run(["python", "europeana_ingest_batch.py"], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Ingestion script failed: {e}")
    logging.info("Sleeping for 1 hour...")
    time.sleep(3600)










# vecchio:
# import requests
# import json
# from kafka import KafkaProducer
# from datetime import datetime
# import os

# # Kafka producer config
# producer = KafkaProducer(
#     bootstrap_servers=["kafka:9092", "kafka2:9093", "kafka3:9094"],
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# API_KEY = "ianlefuck"

# # Configura numero di query da processare per run
# CHUNK_SIZE = 2

# # Carica tutte le query da file
# with open("queries.txt", "r") as f:
#     queries = [line.strip() for line in f if line.strip()]

# total_queries = len(queries)

# # Leggi la posizione dell'ultimo offset (se esiste)
# offset_file = "state/offset.txt"
# if os.path.exists(offset_file):
#     with open(offset_file, "r") as f:
#         offset = int(f.read().strip())
# else:
#     offset = 0

# # Calcola indice finale del batch corrente
# end = min(offset + CHUNK_SIZE, total_queries)

# # Se arrivi in fondo, ricomincia da 0
# if offset >= total_queries:
#     offset = 0
#     end = CHUNK_SIZE

# print(f"Processing queries {offset} to {end - 1}")

# # Ingestione Europeana per le query del blocco corrente
# for topic in queries[offset:end]:
#     print(f"Query: {topic}")
#     url = f"https://api.europeana.eu/record/v2/search.json?wskey={API_KEY}&query={topic}&rows=10"

#     try:
#         response = requests.get(url)
#         items = response.json().get("items", [])
#     except Exception as e:
#         print(f"Errore su {topic}: {e}")
#         continue

#     for item in items:
#         if "edmIsShownBy" not in item:
#             continue

#         metadata = {
#             "title": item.get("title", [""])[0],
#             "guid": item.get("guid", ""),
#             "image_url": item.get("edmIsShownBy"),
#             "timestamp_created": datetime.utcnow().isoformat(),
#             "query": topic,
#         }

#         producer.send("europeana_metadata", metadata)
#         print(f"Sent: {metadata['title']}")

# # Scrivi nuovo offset nel file (per la prossima run)
# new_offset = end if end < total_queries else 0
# with open(offset_file, "w") as f:
#     f.write(str(new_offset))

# producer.flush()
# print("Run completed")
