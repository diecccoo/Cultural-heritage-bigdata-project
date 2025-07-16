# ogni 6 ore Questo script:
# legge le keyword da queries.txt 
# Controlla da dove deve ripartire (offset.txt)
# Seleziona solo 2 query
# per ogni query, fa una chiamata all’API Europeana
# estrae tot item per query (puoi modificare rows=..)
# costruisce un dizionario JSON con i metadati (titolo, immagine, GUID…)
# invia ogni JSON su Kafka → topic: europeana_metadata
# Aggiorna offset.txt con la nuova posizione
# Aspetta .. ore
import time 
import subprocess
import logging

MAX_RETRIES = 10
SLEEP_BETWEEN_RETRIES = 15
WAIT_AFTER_SUCCESS = 30 #secondi

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

def run_script_with_retries():
    retry_count = 0

    while retry_count < MAX_RETRIES:
        try:
            logging.info("🚀 Starting europeana_ingest_batch.py (attempt #%d)...", retry_count + 1)
            subprocess.run(["python", "europeana_ingest_batch.py"], check=True)
            logging.info("✅ Script executed successfully.")
            return True
        except subprocess.CalledProcessError as e:
            retry_count += 1
            wait_time = SLEEP_BETWEEN_RETRIES * retry_count
            logging.warning(f"❌ Script failed (attempt #{retry_count}): {e}")
            logging.info(f"⏳ Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    logging.error("🚨 Script failed after %d attempts. Skipping this round.", MAX_RETRIES)
    return False

# Loop infinito
while True:
    success = run_script_with_retries()
    if success:
        logging.info("🛌 Sleeping for ... minutes before next execution...")
        time.sleep(WAIT_AFTER_SUCCESS)
    else:
        logging.info("🔁 Will retry from scratch in next cycle.")
