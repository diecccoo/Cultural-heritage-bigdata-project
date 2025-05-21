import sys
import requests
import json
from io import BytesIO
from minio import Minio
from kafka import KafkaProducer
import time
from kafka.errors import NoBrokersAvailable



# --- Config ---
API_KEY = "ianlefuck"
QUERY = sys.argv[1] if len(sys.argv) > 1 else "rococo"
BUCKET = "europeana-data"

# --- Init MinIO client ---
client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)


# --- Ensure bucket exists ---
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

# --- Europeana API call ---
url = f"https://api.europeana.eu/record/v2/search.json?wskey={API_KEY}&query={QUERY}&type=IMAGE&media=true&rows=100"
response = requests.get(url)
results = response.json().get("items", [])


# --- Retry KafkaProducer until available ---
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except NoBrokersAvailable:
        print(f"[!] Kafka not available yet. Retry {i+1}/10")
        time.sleep(5)
else:
    raise RuntimeError("❌ Could not connect to Kafka after multiple retries")


print(f"Found {len(results)} items for topic '{QUERY}'")


# --- Upload images + metadata ---
for i, item in enumerate(results):
    img_url = item.get("edmPreview", [None])[0]
    if not img_url:
        print(f"[!] Skipping item {i} — no image found.")
        continue

    # Define image and metadata paths
    img_name = f"{QUERY}/images/{QUERY}_{i}.jpg"
    json_name = f"{QUERY}/metadata/{QUERY}_{i}.json"

    try:
        # --- Check and upload image ---
        try:
            client.stat_object(BUCKET, img_name)
            print(f"[i] Skipping existing image: {img_name}")
        except:
            img_response = requests.get(img_url, stream=True)
            img_data = BytesIO(img_response.content)
            client.put_object(
                bucket_name=BUCKET,
                object_name=img_name,
                data=img_data,
                length=len(img_response.content),
                content_type="image/jpeg"
            )

        # --- Prepare metadata ---
        metadata = {
            "title": item.get("title", [""])[0],
            "creator": item.get("dcCreator", [""])[0],
            "guid": item.get("guid"),
            "type": item.get("type"),
            "rights": item.get("rights", [""])[0],
            "timestamp_created": item.get("timestamp_created", ""),
            "dataProvider": item.get("dataProvider", [""])[0]
        }

        # --- Check and upload metadata ---
        try:
            client.stat_object(BUCKET, json_name)
            print(f"[i] Skipping existing metadata: {json_name}")
        except:
            json_data = BytesIO(json.dumps(metadata, indent=2).encode("utf-8"))
            client.put_object(
                bucket_name=BUCKET,
                object_name=json_name,
                data=json_data,
                length=len(json_data.getbuffer()),
                content_type="application/json"
            )
            producer.send('europeana-metadata', value=metadata)
            producer.flush()

        print(f"[✓] Present: {img_name} and {json_name}")

    except Exception as e:
        print(f"[X] Failed on item {i}: {e}")