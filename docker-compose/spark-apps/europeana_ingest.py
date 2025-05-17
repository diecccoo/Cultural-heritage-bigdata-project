import sys
import requests
import json
from io import BytesIO
from minio import Minio

# --- Config ---
API_KEY = "ianlefuck"
QUERY = sys.argv[1] if len(sys.argv) > 1 else "rococo"
BUCKET = "europeana-data"

# --- Init MinIO client ---
client = Minio(
    "localhost:9000",
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

print(f"Found {len(results)} items for topic '{QUERY}'")

# --- Upload images + metadata ---
for i, item in enumerate(results):
    img_url = item.get("edmPreview", [None])[0]
    if not img_url:
        print(f"[!] Skipping item {i} — no image found.")
        continue

    try:
        # Download image
        img_response = requests.get(img_url, stream=True)
        img_data = BytesIO(img_response.content)
        img_name = f"{QUERY}/images/{QUERY}_{i}.jpg"
        client.put_object(
            bucket_name=BUCKET,
            object_name=img_name,
            data=img_data,
            length=len(img_response.content),
            content_type="image/jpeg"
        )

        # Prepare metadata
        metadata = {
            "title": item.get("title", [""])[0],
            "creator": item.get("dcCreator", [""])[0],
            "guid": item.get("guid"),
            "type": item.get("type"),
            "rights": item.get("rights", [""])[0],
            "timestamp_created": item.get("timestamp_created", ""),
            "dataProvider": item.get("dataProvider", [""])[0]
        }
        json_data = BytesIO(json.dumps(metadata, indent=2).encode("utf-8"))
        json_name = f"{QUERY}/metadata/{QUERY}_{i}.json"
        client.put_object(
            bucket_name=BUCKET,
            object_name=json_name,
            data=json_data,
            length=len(json_data.getbuffer()),
            content_type="application/json"
        )

        print(f"[✓] Uploaded: {img_name} and {json_name}")

    except Exception as e:
        print(f"[!] Error on item {i}: {e}")
