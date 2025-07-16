# Questo script Python serve per scaricare oggetti culturali (con immagini o testo) dall'API Europeana, uno alla volta per ciascun provider, e inviarli su Kafka (topic europeana_metadata). Tiene traccia di ciò che ha già scaricato per evitare duplicati e salva il suo stato per continuare nel tempo.
# Estrae metadati Solo se c’è edmIsShownBy e guid nuovo
import requests
import json
import logging
import os
import time
from kafka import KafkaProducer
from datetime import datetime
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

# --- Configurazione ---
API_KEY = os.getenv("API_KEY")
ROWS_PER_PAGE = 100  #sconsigliato da europeana sopra i 100
MAX_PAGES_PER_HOUR = 3  # esempio: 100 (rows x page) x 20 = 2000 oggetti/x batch
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
MAX_CONSECUTIVE_FAILURES = 5
LANGUAGE = "en"


PROVIDERS = [
    "Wellcome Collection",
    "The European Library",
    "CultureGrid",
    "CARARE",
    "Digital Repository of Ireland",
    "MUSEU",
    "European Fashion Heritage Association",
    "LoCloud",
    "PHOTOCONSORTIUM",
    "OpenUp!",
    "EUscreen",
    "Dutch Collections for Europe",
    "Heritage Malta",
    "EuropeanaTravel",
    "Deputy Ministry of Culture, Republic of Cyprus",
    "Jewish Heritage Network",
    "Europeana 280"
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
    bootstrap_servers=["kafka:9092"], 
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Carica stato ---
if os.path.exists(OFFSET_FILE):
    with open(OFFSET_FILE, "r") as f:
        state = json.load(f)
else:
    state = {"provider_index": 0}

if os.path.exists(downloaded_guids_file):
    with open(downloaded_guids_file, "r") as f:
        downloaded_guids = set(line.strip() for line in f)
else:
    downloaded_guids = set()


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
            "(TYPE:IMAGE)",
            f"LANGUAGE:{LANGUAGE}"
        ]
    }
    
    # Usa requests.get con params invece di costruire l'URL manualmente
    return base_url, params

# --- Loop su max pagine ---
provider = PROVIDERS[state["provider_index"]]
logging.info(f"🔍 Inizio: provider '{provider}'")

consecutive_failures = 0
total_saved = 0

filter_query = (
    f'PROVIDER:"{provider}" AND (TYPE:IMAGE) AND LANGUAGE:{LANGUAGE}'
)

cursor = "*"  
pages_fetched = 0



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
        # Aggiorna subito lo stato per passare al prossimo provider
        state["provider_index"] = (state["provider_index"] + 1) % len(PROVIDERS)
        break

    # Processa gli items
    for item in items:
        guid = item.get("guid", "")
        if not guid or guid in downloaded_guids:
            continue
        image_url = item.get("edmIsShownBy") or item.get("isShownBy")
        if not image_url:
            continue 

        metadata = {
            "title": item.get("title", [""])[0],
            "guid": guid,
            "image_url": image_url,
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

    # Aggiungi un piccolo delay per non sovraccaricare l'API
    time.sleep(2)

# Se finito o interrotto → passa al prossimo provider
state["provider_index"] = (state["provider_index"] + 1) % len(PROVIDERS)
logging.info(f"➡️ Prossima volta useremo provider '{PROVIDERS[state['provider_index']]}'")

# --- Salva stato e guid ---
with open(OFFSET_FILE, "w") as f:
    json.dump(state, f)

with open(downloaded_guids_file, "w") as f:
    for guid in downloaded_guids:
        f.write(f"{guid}\n")

logging.info(f"🏁 Script completato. Totale oggetti salvati: {total_saved}")