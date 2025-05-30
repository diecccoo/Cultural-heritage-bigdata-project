import boto3
from botocore.exceptions import ClientError

# ---------------- Configuration ----------------
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET_NAME = "heritage"

# ---------------- Setup Client ----------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

# ---------------- Bucket Creation ----------------
def create_bucket(s3, bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"ℹ️ Bucket '{bucket_name}' already exists.")
        else:
            raise

# ---------------- Folder Initialization ----------------
def create_folder(s3, bucket, folder_path):
    # Simulate folders by uploading an empty object with a trailing slash
    s3.put_object(Bucket=bucket, Key=folder_path)
    print(f"📁 Initialized folder: {folder_path}")

def main():
    s3 = get_s3_client()
    create_bucket(s3, BUCKET_NAME)

    folders = [
        "raw/metadata/",
        "raw/images/",
        "cleansed/",
        "curated/"
    ]

    for folder in folders:
        create_folder(s3, BUCKET_NAME, folder)

if __name__ == "__main__":
    main()
