import sys
import requests
import json
import pyarrow
from io import BytesIO
from minio import Minio
import pandas as pd
from datetime import date

# --- Configuration ---
API_KEY = "ianlefuck"
QUERY = sys.argv[1] if len(sys.argv) > 1 else "rococo"
BUCKET = "europeana-data"

# --- Initialize MinIO client ---
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

# --- Collect all metadata ---
metadata_list = []

for i, item in enumerate(results):
    img_url = item.get("edmPreview", [None])[0]
    if not img_url:
        print(f"[!] Skipping item {i} — no image found.")
        continue

    img_name = f"{QUERY}/images/{QUERY}_{i}.jpg"

    try:
        # --- Upload image only if not already in MinIO ---
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

        # --- Append metadata ---
        # --- Build metadata dict ---
        metadata = {
            "title": item.get("title", [""])[0],
            "creator": item.get("dcCreator", [""])[0],
            "guid": item.get("guid"),
            "type": item.get("type"),
            "rights": item.get("rights", [""])[0],
            "timestamp_created": item.get("timestamp_created", ""),
            "dataProvider": item.get("dataProvider", [""])[0],
            "image_url": img_url,
            "image_path": img_name,
            "query": QUERY,
            "date": str(date.today())
        }

        # --- Save .json metadata to MinIO ---
        json_name = f"{QUERY}/metadata/{QUERY}_{i}.json"
        json_data = BytesIO(json.dumps(metadata, indent=2).encode("utf-8"))
        client.put_object(
            bucket_name=BUCKET,
            object_name=json_name,
            data=json_data,
            length=len(json_data.getbuffer()),
            content_type="application/json"
        )

        # --- Add to list for Parquet ---
        metadata_list.append(metadata)
        print(f"[✓] Processed: {img_name} and {json_name}")

       
    except Exception as e:
        print(f"[!] Failed on item {i}: {e}")

# --- Save partitioned Parquet to MinIO (overwrite each run) ---
if metadata_list:
    df = pd.DataFrame(metadata_list)
    df = df.drop_duplicates(subset='guid')

    # Prepare Parquet buffer
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Partitioned path: metadata-parquet/query=rococo/date=2025-05-21/
    parquet_path = f"metadata-parquet/query={QUERY}/date={date.today()}/{QUERY}_metadata.parquet"

    client.put_object(
        bucket_name=BUCKET,
        object_name=parquet_path,
        data=BytesIO(parquet_buffer.getvalue()),
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )

    print(f"[✓] Parquet metadata saved to MinIO: {parquet_path}")
else:
    print("[!] No metadata collected — Parquet not created.")