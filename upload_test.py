import boto3
import requests
import json
import uuid
import time
from io import BytesIO
from kafka import KafkaProducer
from botocore.exceptions import ClientError

# === CONFIG ===
bucket_name = "heritage-scans"
image_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b6/Image_created_with_a_mobile_phone.png/640px-Image_created_with_a_mobile_phone.png"
object_key = str(uuid.uuid4()) + ".jpg"

# === CONNECT TO MINIO ===
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123"
)

# === CREATE BUCKET IF NOT EXISTS ===
try:
    s3.head_bucket(Bucket=bucket_name)
except ClientError:
    print(f"ℹ️ Bucket '{bucket_name}' non trovato. Creazione...")
    s3.create_bucket(Bucket=bucket_name)

# === DOWNLOAD IMAGE IN MEMORY AND UPLOAD TO MINIO ===


try:
    headers = {"User-Agent": "CulturalHeritageBot/1.0 (https://www.unitn.it/it)"}
    response = requests.get(image_url, stream=True, headers=headers)
except Exception as e:
    print(f"❌ Errore nel download: {e}")
    exit(1)

# response = requests.get(image_url, stream=True)
# if response.status_code == 200:
#     s3.upload_fileobj(BytesIO(response.content), bucket_name, object_key)
#     print(f"✅ Immagine caricata come {object_key}")
# else:
#     print("❌ Errore nel download dell'immagine.")
#     exit(1)

# === PREPARE MESSAGE FOR KAFKA ===
record = {
    "scanId": object_key[:-4],
    "uri": f"s3://{bucket_name}/{object_key}",
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "mime": "image/jpeg"
}

# === SEND TO KAFKA ===
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

producer.send("new-scans", record)
producer.flush()

print("📤 Kafka message inviato:")
print(json.dumps(record, indent=2))