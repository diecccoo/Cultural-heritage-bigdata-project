import requests
import json
import logging
import os
import time
from kafka import KafkaProducer
from datetime import datetime

# Crea la cartella state/ se non esiste
os.makedirs("state", exist_ok=True)


# Configurazione del logging
logging.basicConfig(
    filename='europeana_ingestion.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Configurazione del produttore Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092", "kafka2:9093", "kafka3:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

API_KEY = "ianlefuck"
ROWS_PER_PAGE = 100
MAX_ITEMS_PER_QUERY = 2000
MAX_PAGES = MAX_ITEMS_PER_QUERY // ROWS_PER_PAGE
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

# Carica le query dal file
with open("queries.txt", "r", encoding="utf-8") as f:
    queries = [line.strip() for line in f if line.strip()]

total_queries = len(queries)

# Legge l'offset corrente
offset_file = "state/offset.txt"
if os.path.exists(offset_file):
    with open(offset_file, "r") as f:
        offset = int(f.read().strip())
else:
    offset = 0

# Determina la query corrente
current_query = queries[offset % total_queries]
logging.info(f"Processing query: {current_query}")

# Carica i GUID già scaricati
downloaded_guids_file = "state/downloaded_guids.txt"
if os.path.exists(downloaded_guids_file):
    with open(downloaded_guids_file, "r") as f:
        downloaded_guids = set(line.strip() for line in f if line.strip())
else:
    downloaded_guids = set()

# Funzione per effettuare richieste con retry e backoff esponenziale
def fetch_with_retry(url):
    retries = 0
    backoff = 1
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed: {e}. Retrying in {backoff} seconds...")
            time.sleep(backoff)
            retries += 1
            backoff *= BACKOFF_FACTOR
    logging.error(f"Failed to fetch data after {MAX_RETRIES} retries.")
    return None

# Processa fino a MAX_PAGES di risultati
for page in range(MAX_PAGES):
    start = page * ROWS_PER_PAGE
    url = (
        f"https://api.europeana.eu/record/v2/search.json"
        f"?wskey={API_KEY}&query={current_query}"
        f"&rows={ROWS_PER_PAGE}&start={start}&qf=TYPE:IMAGE"
    )

    data = fetch_with_retry(url)
    if data is None:
        logging.error(f"Skipping query '{current_query}' due to repeated failures.")
        break

    items = data.get("items", [])
    if not items:
        logging.info(f"No items found for query '{current_query}' at page {page}.")
        break

    for item in items:
        guid = item.get("guid", "")
        if not guid or guid in downloaded_guids:
            continue  # Salta i duplicati

        if "edmIsShownBy" not in item:
            continue  # Salta se manca l'immagine

        metadata = {
            "title": item.get("title", [""])[0],
            "guid": item.get("guid", ""),
            "image_url": item.get("edmIsShownBy"),
            "timestamp_created": datetime.utcnow().isoformat(),
            "query": current_query,
            "description": item.get("dcDescription", [""])[0] if "dcDescription" in item else None,
            "creator": item.get("dcCreator", [""])[0] if "dcCreator" in item else None,
            "subject": item.get("dcSubject", [""])[0] if "dcSubject" in item else None,
            "language": item.get("dcLanguage", [""])[0] if "dcLanguage" in item else None,
            "type": item.get("type", ""),
            "format": item.get("dcFormat", [""])[0] if "dcFormat" in item else None,
            "rights": item.get("rights", [""])[0] if "rights" in item else None,
            "provider": item.get("provider", ""),
            "dataProvider": item.get("dataProvider", ""),
            "isShownAt": item.get("edmIsShownAt", ""),
            "isShownBy": item.get("edmIsShownBy", ""),
            "edm_rights": item.get("edmRights", [""])[0] if "edmRights" in item else None,
        }

        try:
            producer.send("europeana_metadata", metadata)
            logging.info(f"Sent: {metadata['title']}")
            downloaded_guids.add(guid)
        except Exception as e:
            logging.error(f"Error sending to Kafka: {e}")

    time.sleep(1)  # Pausa per rispettare i limiti dell'API

# Aggiorna l'offset per la prossima esecuzione
new_offset = (offset + 1) % total_queries
with open(offset_file, "w") as f:
    f.write(str(new_offset))

# Salva i GUID scaricati
with open(downloaded_guids_file, "w") as f:
    for guid in downloaded_guids:
        f.write(f"{guid}\n")

producer.flush()
logging.info("Execution completed.")
