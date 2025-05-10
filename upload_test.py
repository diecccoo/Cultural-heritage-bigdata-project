import boto3

import json
import uuid
import time
from kafka import KafkaProducer

# Upload to MinIO
s3 = boto3.client("s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123"
)

scan_id = str(uuid.uuid4()) + ".jpg"
s3.upload_file("test.jpg", "heritage-scans", scan_id)

# Produce Kafka message
record = {
    "scanId": scan_id[:-4],
    "uri": f"s3://heritage-scans/{scan_id}",
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "mime": "image/jpeg"
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

producer.send("new-scans", record)
producer.flush()
print("✔️ Uploaded and sent:", record)

