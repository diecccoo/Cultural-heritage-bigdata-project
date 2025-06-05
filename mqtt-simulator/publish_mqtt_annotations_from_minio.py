import json
import time
import random
from datetime import datetime
from minio import Minio
from paho.mqtt import client as mqtt_client

# ----- CONFIGURAZIONI -----
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "heritage"
IMAGES_PREFIX = "raw/images/"

MQTT_BROKER = "localhost"  # o "mqtt" se sei dentro container
MQTT_PORT = 1883
MQTT_TOPIC = "heritage/annotations"

# ----- CONNECT TO MINIO -----
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Scarica tutti gli object_id dal path raw/images/
objects = minio_client.list_objects(MINIO_BUCKET, prefix=IMAGES_PREFIX, recursive=True)
object_ids = [obj.object_name.split("/")[-1].rsplit(".", 1)[0] for obj in objects]

if not object_ids:
    print("⛔ Nessuna immagine trovata in MinIO!")
    exit(1)

# ----- VALORI SIMULATI -----
user_ids = [
    "museumlover", "artfan21", "picasso88", "anon123", "visitorX", "curator42",
    "historybuff", "artexpert", "randomnick", "user_xyz"
]

tags_pool = [
    "religious", "sculpture", "wood", "metal", "ceramic", "baroque", "portrait", "abstract", "medieval", "painting",
    "mythological", "restoration", "landscape", "inscription", "damaged", "intact", "framed", "gilded", "bronze", "engraved"
]

comments_pool = [
    "Bellissimi dettagli in legno.",
    "Mi ricorda l'arte barocca.",
    "Opera molto interessante.",
    "Colori ancora vividi.",
    "Forse restaurata di recente?",
    "Interessante uso dei materiali.",
    "Misteriosa, non riesco a datarla.",
    "La cornice sembra originale.",
    "Tipico stile gotico.",
    "Non capisco se sia legno o bronzo."
]

locations_pool = [
    "Rome", "Florence", "Venice", "Milan", "Naples", "Trento", "Paris", "Berlin", "Vienna", "New York"
]

# ----- MQTT SETUP -----
client = mqtt_client.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def random_annotation():
    return {
        "object_id": random.choice(object_ids),
        "user_id": random.choice(user_ids),
        "tags": random.sample(tags_pool, k=random.randint(2, 4)),
        "comment": random.choice(comments_pool),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": random.choice(locations_pool)
    }

# ----- PUBBLICA OGNI 2 SECONDI -----
try:
    print("Simulatore MQTT da immagini MinIO attivo. CTRL+C per fermare.")
    while True:
        annotation = random_annotation()
        payload = json.dumps(annotation)
        client.publish(MQTT_TOPIC, payload=payload)
        print("📤 Pubblicato:", payload)
        time.sleep(2)
except KeyboardInterrupt:
    print("\nSimulazione interrotta manualmente.")
