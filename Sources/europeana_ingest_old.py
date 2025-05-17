#  What this script is doing:
# Items related to "rococo" (Rococò artworks)
# Only items of type IMAGE
# Limited to 20 results
# Including image previews (media=true)

import requests
import json
from io import BytesIO
from minio import Minio

# Config
API_KEY = "ianlefuck"
QUERY = "rococo"
BUCKET = "europeana-rococo"

# Initialize MinIO client
client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

# Create bucket if it doesn't exist
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

# Call Europeana API
url = f"https://api.europeana.eu/record/v2/search.json?wskey={API_KEY}&query={QUERY}&type=IMAGE&media=true&rows=100"
response = requests.get(url)
results = response.json().get("items", [])

print(f"Found {len(results)} items from Europeana")

# Process each item
for i, item in enumerate(results):
    img_url = item.get("edmPreview", [None])[0]
    if not img_url:
        print(f"Skipping item {i}: no image found.")
        continue

    try:
        # Download image as stream
        img_response = requests.get(img_url, stream=True)
        img_data = BytesIO(img_response.content)
        img_name = f"images/rococo_{i}.jpg"
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
        json_name = f"metadata/rococo_{i}.json"
        client.put_object(
            bucket_name=BUCKET,
            object_name=json_name,
            data=json_data,
            length=len(json_data.getbuffer()),
            content_type="application/json"
        )

        print(f"[✓] Uploaded: {img_name} + metadata")

    except Exception as e:
        print(f"[!] Failed on item {i}: {e}")
