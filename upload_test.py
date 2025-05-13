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
image_url = "https://homepages.cae.wisc.edu/~ece533/images/airplane.png"  # usa questo per ora
headers = {"User-Agent": "CulturalHeritageBot/1.0 (https://your-university.it)"}
object_key = str(uuid.uuid4()) + ".jpg"

# === SETUP MINIO CLIENT ===
s3 = boto3.client("s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123"
)

# === CREA BUCKET SE NON ESISTE ===
try:
    s3.head_bucket(Bucket=bucket_name)
except ClientError:
    print(f"📂 Creo bucket '{bucket_name}'...")
    s3.create_bucket(Bucket=bucket_name)

# === SCARICA IMMAGINE IN RAM ===
print("⬇️ Download immagine da:", image_url)
response = requests.get(image_url, stream=True, headers=headers)
response.raise_for_status()
img_data = BytesIO(response.content)

# === UPLOAD SU MINIO ===
print("⬆️ Upload su MinIO come:", object_key)
s3.upload_fileobj(img_data, bucket_name, object_key)

# === INVIA MESSAGGIO SU KAFKA ===
record = {
    "scanId": object_key[:-4],
    "uri": f"s3://{bucket_name}/{object_key}",
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "mime": "image/jpeg"
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

producer.send("new-scans", record)
producer.flush()

print("📤 Kafka message inviato:")
print(json.dumps(record, indent=2))
print("✅ Upload e invio completati con successo.")