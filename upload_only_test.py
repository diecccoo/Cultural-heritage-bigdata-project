import boto3
import requests
from io import BytesIO

# === CONFIG ===
bucket_name = "heritage-scans"
object_key = "prova.jpg"
image_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b6/Image_created_with_a_mobile_phone.png/640px-Image_created_with_a_mobile_phone.png"
headers = {"User-Agent": "CulturalHeritageBot/1.0 (https://your-university.it)"}

# === SETUP S3 ===
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123"
)

# === TEST: Crea bucket se non esiste ===
try:
    s3.head_bucket(Bucket=bucket_name)
except:
    print(f"📂 Creo bucket {bucket_name}")
    s3.create_bucket(Bucket=bucket_name)

# === DOWNLOAD + UPLOAD ===
print("⬇️ Scarico immagine da URL...")
response = requests.get(image_url, stream=True, headers=headers)
response.raise_for_status()

print(f"⬆️ Carico su MinIO come: {object_key}")
s3.upload_fileobj(BytesIO(response.content), bucket_name, object_key)

print("✅ Upload completato con successo.")
