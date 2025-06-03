import requests
import json
from kafka import KafkaProducer
from datetime import datetime
import time

# Kafka Producer con serializzazione JSON
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092", "kafka2:9093", "kafka3:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Inserisci qui la tua API key
API_KEY = "ianlefuck"  

# Carica le query da file
with open("queries.txt", "r") as f:
    queries = [line.strip() for line in f if line.strip()]

# Per ogni query, esegui ricerca su Europeana e invia risultati a Kafka
for topic in queries:
    print(f"Query: {topic}")
    url = f"https://api.europeana.eu/record/v2/search.json?wskey={API_KEY}&query={topic}&rows=100"

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Errore per {topic}")
        continue

    items = response.json().get("items", [])
    for item in items:
        if "edmIsShownBy" not in item:
            continue  # niente immagine
        
        metadata = {
            "title": item.get("title", [""])[0],
            "guid": item.get("guid", ""),
            "image_url": item.get("edmIsShownBy"),
            "timestamp_created": datetime.utcnow().isoformat(),
            "query": topic,
        }

        # Invia JSON al topic Kafka
        producer.send("europeana_metadata", metadata)
        print(f"Inviato per: {metadata['title']}")

producer.flush()
print("Completato")
