import requests
import json
import logging
import os
import time
from kafka import KafkaProducer
from datetime import datetime
from urllib.parse import quote

# --- Configurazione ---
API_KEY = "ianlefuck"
ROWS_PER_PAGE = 100
MAX_PAGES_PER_HOUR = 20  # 100 x 20 = 2000 oggetti/h
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
MAX_CONSECUTIVE_FAILURES = 10
LANGUAGE = "en"
TYPES = ["IMAGE", "TEXT"]

PROVIDERS = [
    "Wellcome Collection",
    "The European Library",
    "CultureGrid",
    "AthenaPlus",
    "CARARE",
    "Digital Repository of Ireland",
    "MUSEU",
    "National Library of Wales",
    "Jewish Heritage Network",
    "National Library of Scotland",
    "European Fashion Heritage Association",
    "LoCloud",
    "PHOTOCONSORTIUM"
]

# --- Paths ---
STATE_PATH = "state"
os.makedirs(STATE_PATH, exist_ok=True)

OFFSET_FILE = os.path.join(STATE_PATH, "europeana_offset.json")
downloaded_guids_file = os.path.join(STATE_PATH, "downloaded_guids.txt")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# --- Kafka Producer ---
producer = KafkaProducer(
    # bootstrap_servers=["kafka:9092", "kafka2:9093", "kafka3:9094"], # con 3 brokers (kafka3)
    bootstrap_servers=["kafka:9092", "kafka2:9093"], # con 2 brokers (kafka2)
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Carica stato ---
if os.path.exists(OFFSET_FILE):
    with open(OFFSET_FILE, "r") as f:
        state = json.load(f)
else:
    state = {"provider_index": 0, "page": 1}

if os.path.exists(downloaded_guids_file):
    with open(downloaded_guids_file, "r") as f:
        downloaded_guids = set(line.strip() for line in f)
else:
    downloaded_guids = set()

# --- Retry con backoff ---
# def fetch_with_retry(url):
#     retries = 0
#     backoff = 1
#     while retries < MAX_RETRIES:
#         try:
#             response = requests.get(url, timeout=10)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             logging.warning(f"Request failed: {e}. Retrying in {backoff} seconds...")
#             time.sleep(backoff)
#             retries += 1
#             backoff *= BACKOFF_FACTOR
#     logging.error("❌ Failed to fetch data after max retries.")
#     return None

# # --- Loop su max pagine ---
# provider = PROVIDERS[state["provider_index"]]
# page = max(1, state["page"])  # Fix per evitare start=0
# logging.info(f"🔍 Inizio: provider '{provider}' | dalla pagina {page}")

# consecutive_failures = 0
# total_saved = 0

# query = "*:*"
# filter_query = (
#     f'PROVIDER:"{provider}" AND (TYPE:IMAGE OR TYPE:TEXT) AND LANGUAGE:{LANGUAGE}'
# )
# qf_string = f"&qf={quote(filter_query)}"
# cursor = "*"  # Inizio dello scroll
# pages_fetched = 0

# # while pages_fetched < MAX_PAGES_PER_HOUR:
#     saved_this_round = 0
#     logging.info(f"🔁 Scrolling page {pages_fetched + 1} (provider: {provider})")

#     url = (
#         f"https://api.europeana.eu/record/v2/search.json"
#         f"?wskey={API_KEY}&query={quote(query)}&rows={ROWS_PER_PAGE}"
#         f"&profile=rich&cursor={cursor}{qf_string}"
#     )

#     data = fetch_with_retry(url)
#     if data is None:
#         logging.error("⛔ API call failed.")
#         consecutive_failures += 1
#         if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
#             logging.error("🚨 Troppe query fallite consecutive. Arresto script.")
#             break
#         continue

#     items = data.get("items", [])
#     if not items:
#         logging.warning("⚠️ Nessun oggetto restituito. Cambio provider.")
#         break

#     for item in items:
#         guid = item.get("guid", "")
#         if not guid or guid in downloaded_guids:
#             continue
#         if "edmIsShownBy" not in item:
#             continue

#         metadata = {
#             "title": item.get("title", [""])[0],
#             "guid": guid,
#             "image_url": item.get("edmIsShownBy"),
#             "timestamp_created": datetime.utcnow().isoformat(),
#             "provider": provider,
#             "description": item.get("dcDescription", [""])[0] if "dcDescription" in item else None,
#             "creator": item.get("dcCreator", [""])[0] if "dcCreator" in item else None,
#             "subject": item.get("dcSubject", [""])[0] if "dcSubject" in item else None,
#             "language": item.get("dcLanguage", [""])[0] if "dcLanguage" in item else None,
#             "type": item.get("type", ""),
#             "format": item.get("dcFormat", [""])[0] if "dcFormat" in item else None,
#             "rights": item.get("rights", [""])[0] if "rights" in item else None,
#             "dataProvider": item.get("dataProvider", ""),
#             "isShownAt": item.get("edmIsShownAt", ""),
#             "isShownBy": item.get("edmIsShownBy", ""),
#             "edm_rights": item.get("edmRights", [""])[0] if "edmRights" in item else None,
#         }

#         try:
#             producer.send("europeana_metadata", metadata)
#             downloaded_guids.add(guid)
#             saved_this_round += 1
#             total_saved += 1
#         except Exception as e:
#             logging.error(f"Kafka send error: {e}")

#     producer.flush()
#     logging.info(f"✅ Salvati {saved_this_round} nuovi oggetti (pagina {pages_fetched + 1})")

#     #  Aggiorna il cursore per continuare lo scroll
#     cursor = data.get("nextCursor")
#     if not cursor:
#         logging.info("🛑 Fine dello scrolling: nessun nextCursor restituito.")
#         break

#     pages_fetched += 1
#     page += 1
#     state["page"] = page


# In europeana-scraper/europeana_ingest_batch.py

# --- Configurazione ---
# Sostituisci con la tua API key valida ottenuta da Europeana
API_KEY = "ianlefuck" 

# Modifica la costruzione dell'URL per renderla più robusta
def build_europeana_url(query, provider, cursor):
    base_url = "https://api.europeana.eu/record/v2/search.json"
    
    # Costruisci i parametri della query in modo più pulito
    params = {
        "wskey": API_KEY,
        "query": query,
        "rows": ROWS_PER_PAGE,
        "profile": "rich",
        "cursor": cursor,
        "qf": [
            f'PROVIDER:"{provider}"',
            "(TYPE:IMAGE OR TYPE:TEXT)",
            f"LANGUAGE:{LANGUAGE}"
        ]
    }
    
    # Usa requests.get con params invece di costruire l'URL manualmente
    return base_url, params

# --- Loop su max pagine ---
provider = PROVIDERS[state["provider_index"]]
page = max(1, state["page"])  # Fix per evitare start=0
logging.info(f"🔍 Inizio: provider '{provider}' | dalla pagina {page}")

consecutive_failures = 0
total_saved = 0

query = "*:*"
filter_query = (
    f'PROVIDER:"{provider}" AND (TYPE:IMAGE OR TYPE:TEXT) AND LANGUAGE:{LANGUAGE}'
)
qf_string = f"&qf={quote(filter_query)}"
cursor = "*"  # Inizio dello scroll
pages_fetched = 0


# --- Modifica la funzione fetch_with_retry ---
def fetch_with_retry(base_url, params):
    retries = 0
    backoff = 1
    while retries < MAX_RETRIES:
        try:
            response = requests.get(
                base_url,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed: {e}. Retrying in {backoff} seconds...")
            time.sleep(backoff)
            retries += 1
            backoff *= BACKOFF_FACTOR
    logging.error("❌ Failed to fetch data after max retries.")
    return None

# --- Nel loop principale, modifica la chiamata API così ---
# --- Nel loop principale, modifica la chiamata API così ---
while pages_fetched < MAX_PAGES_PER_HOUR:
    saved_this_round = 0
    logging.info(f"🔁 Scrolling page {pages_fetched + 1} (provider: {provider})")

    base_url, params = build_europeana_url("*:*", provider, cursor)
    data = fetch_with_retry(base_url, params)
    
    if data is None:
        logging.error("⛔ API call failed.")
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logging.error("🚨 Troppe query fallite consecutive. Arresto script.")
            break
        continue

    items = data.get("items", [])
    if not items:
        logging.warning("⚠️ Nessun oggetto restituito. Cambio provider.")
        break

    # Processa gli items
    for item in items:
        guid = item.get("guid", "")
        if not guid or guid in downloaded_guids:
            continue
        if "edmIsShownBy" not in item:
            continue

        metadata = {
            "title": item.get("title", [""])[0],
            "guid": guid,
            "image_url": item.get("edmIsShownBy"),
            "timestamp_created": datetime.utcnow().isoformat(),
            "provider": provider,
            "description": item.get("dcDescription", [""])[0] if "dcDescription" in item else None,
            "creator": item.get("dcCreator", [""])[0] if "dcCreator" in item else None,
            "subject": item.get("dcSubject", [""])[0] if "dcSubject" in item else None,
            "language": item.get("dcLanguage", [""])[0] if "dcLanguage" in item else None,
            "type": item.get("type", ""),
            "format": item.get("dcFormat", [""])[0] if "dcFormat" in item else None,
            "rights": item.get("rights", [""])[0] if "rights" in item else None,
            "dataProvider": item.get("dataProvider", ""),
            "isShownAt": item.get("edmIsShownAt", ""),
            "isShownBy": item.get("edmIsShownBy", ""),
            "edm_rights": item.get("edmRights", [""])[0] if "edmRights" in item else None,
        }

        try:
            producer.send("europeana_metadata", metadata)
            downloaded_guids.add(guid)
            saved_this_round += 1
            total_saved += 1
        except Exception as e:
            logging.error(f"Kafka send error: {e}")

    producer.flush()
    logging.info(f"✅ Salvati {saved_this_round} nuovi oggetti (pagina {pages_fetched + 1})")

    # Aggiorna il cursore per la prossima pagina
    cursor = data.get("nextCursor")
    if not cursor:
        logging.info("🛑 Fine dello scrolling: nessun nextCursor restituito.")
        break

    pages_fetched += 1
    page += 1
    state["page"] = page

    # Aggiungi un piccolo delay per non sovraccaricare l'API
    time.sleep(1)

# Se finito o interrotto → passa al prossimo provider
if total_saved == 0 or consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
    state["provider_index"] = (state["provider_index"] + 1) % len(PROVIDERS)
    state["page"] = 1  # Parte dalla pagina 1
    logging.info(f"➡️ Prossima volta useremo provider '{PROVIDERS[state['provider_index']]}'")

# --- Salva stato e guid ---
with open(OFFSET_FILE, "w") as f:
    json.dump(state, f)

with open(downloaded_guids_file, "w") as f:
    for guid in downloaded_guids:
        f.write(f"{guid}\n")

logging.info(f"🏁 Script completato. Totale oggetti salvati: {total_saved}")


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
