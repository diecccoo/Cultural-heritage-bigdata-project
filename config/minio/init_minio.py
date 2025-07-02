import time
import boto3
from botocore.exceptions import ClientError

# ---------------- Configuration ----------------
MINIO_ENDPOINT = "http://minio:9000"  # container name, non localhost!
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET_NAME = "heritage"

# ---------------- Wait for MinIO ----------------
def wait_for_minio():
    while True:
        try:
            client = get_s3_client()
            client.list_buckets()
            print("MinIO is ready.")
            return
        except Exception as e:
            print("Waiting for MinIO to become available...")
            time.sleep(3)

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
        print(f"Bucket '{bucket_name}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            raise

# ---------------- Folder Initialization ----------------
def create_folder(s3, bucket, folder_path):
    s3.put_object(Bucket=bucket, Key=folder_path)
    print(f"Initialized folder: {folder_path}")

def main():
    wait_for_minio()
    s3 = get_s3_client()
    create_bucket(s3, BUCKET_NAME)

    folders = [
        "raw/metadata/europeana_metadata/",
        "raw/metadata/user_generated_content/",
        "raw/images/",
        "cleansed/europeana/",
        "cleansed/user_generated/",
        "curated/join_metadata/"
    ]


    for folder in folders:
        create_folder(s3, BUCKET_NAME, folder)

if __name__ == "__main__":
    main()
