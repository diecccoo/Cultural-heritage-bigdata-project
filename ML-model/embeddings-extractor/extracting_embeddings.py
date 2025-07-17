import os
import time
from datetime import datetime

import torch
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from transformers import CLIPProcessor, CLIPModel
import ast

from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct
from qdrant_client.http.models import PayloadSchemaType

import hashlib

# ========== CONFIGURATION ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
CLEANSED_PATH = "s3a://heritage/cleansed/europeana/"
EMBEDDING_PATH = "s3a://heritage/cleansed/embeddings/"
STATE_FILE_PATH = "s3a://heritage/cleansed/embedding_last_processed.txt"
CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"
SLEEP_SECONDS = 20


LIMIT_RECORDS = 64  # Limit records per Spark cycle (RAM management).
BATCH_SIZE = 16      # Batch size for embedding CLIP

# ========== INITIALIZE SPARK ==========
spark = SparkSession.builder \
    .appName("EmbeddingExtractorCLIP") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ========== INITIALIZE CLIP with GPU/CPU ==========
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[DEVICE] Using device: {device}")

clip_model = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(device)
clip_processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)


# ========== INITIALIZE QDRANT ==========
# Configure Qdrant client (we only call it once).
qdrant = QdrantClient(host="qdrant", port=6333)

# Create the collection if it does not exist (only at startup).
COLLECTION_NAME = "heritage_embeddings"
# Check if collection already exists
if not qdrant.collection_exists(collection_name=COLLECTION_NAME):
    print(f"[QDRANT] Collection '{COLLECTION_NAME}' not found, creating...")
    qdrant.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={
            "image": VectorParams(size=512, distance=Distance.COSINE),
            "combined": VectorParams(size=1024, distance=Distance.COSINE)
        }
    )
    print(f"[QDRANT] Collection '{COLLECTION_NAME}' successfully created.")
else:
    print(f"[QDRANT] Collection '{COLLECTION_NAME}' already exists, skipped the creation.")

# Add payload index on ‘status’ field to filter pending/validated points.
try:
    qdrant.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="status",
        field_schema=PayloadSchemaType.Keyword
    )
    print("[QDRANT] Created payload index on ‘status’ field")
except Exception as e:
    print(f"[QDRANT] Error creating payload index (maybe it already exists):{e}")



# ========== UTILITY ==========
def sanitize_id(guid):
    """
    Converts the GUID (which can be a long URL) to a string compatible with Qdrant
    """
    return hashlib.md5(guid.encode()).hexdigest()


def read_last_processed_guid():
    try:
        import boto3
        s3 = boto3.client('s3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        bucket = STATE_FILE_PATH.replace("s3a://", "").split("/")[0]
        key = "/".join(STATE_FILE_PATH.replace("s3a://", "").split("/")[1:])
        obj = s3.get_object(Bucket=bucket, Key=key)
        guid = obj['Body'].read().decode().strip()
        print(f"[STATE] Last guid processed: {guid}")
        return guid
    except Exception as e:
        print(f"[STATE] No status file (embedding_last_processed.txt) founded. Starting from first record. ({e})")
        return None

def write_last_processed_guid(guid):
    import boto3
    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    bucket = STATE_FILE_PATH.replace("s3a://", "").split("/")[0]
    key = "/".join(STATE_FILE_PATH.replace("s3a://", "").split("/")[1:])
    s3.put_object(Bucket=bucket, Key=key, Body=guid.encode())
    print(f"[STATE] Status file updated with guid: {guid}")

def get_new_records(last_guid):
    df = spark.read.format("delta").load(CLEANSED_PATH)
    if last_guid:
        df = df.filter(col("guid") > last_guid)
    # Limit records to LIMIT_RECORDS per Spark cycle (RAM management)
    df = df.orderBy(col("guid").asc()).limit(LIMIT_RECORDS)
    print(f"[SPARK] Limiting to {LIMIT_RECORDS}  records per cycle for RAM management.")
    return df

def preprocess_text(row):
    # Order: title, subject, creator, type, description
    title = str(row["title"]) if row["title"] else ""
    subject = str(row["subject"]) if row["subject"] else ""
    creator = str(row["creator"]) if row["creator"] else ""
    type_ = str(row["type"]) if row["type"] else ""
    # Cut description to max 150 characters so as not to saturate CLIP tokens
    description = str(row["description"])[:150] if row["description"] else ""

    # concatenates in the new order
    text = " ".join([
        title,
        subject,
        creator,
        type_,
        description
    ])
    return text.strip()

def fetch_image(url):
    import requests
    from PIL import Image
    import io

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": "https://viewer.cbl.ie/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        content_type = response.headers.get("Content-Type", "")
        content_length = len(response.content)
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Content-Type: {content_type}, Bytes: {content_length}")

        if not content_type.startswith("image/"):
            print(f"[WARN] Non è un'immagine valida: {url}")
            print(f"[BODY] {response.text[:300]}")
            return None

        image = Image.open(io.BytesIO(response.content)).convert("RGB")
        return image

    except Exception as e:
        print(f"[ERROR] Exception in image download: {url} ({e})")
        return None


def get_embeddings_batch(texts, images):
    """
    New function for batch embedding with OOM management
    """
    try:
        # Prepare inputs for batch
        inputs = clip_processor(
            text=texts,
            images=images,
            return_tensors="pt",
            padding=True
        )
        
        # Moves inputs to the device (GPU/CPU)
        inputs = {k: v.to(device) for k, v in inputs.items()}
        
        with torch.no_grad():
            #  OOM with try/except
            try:
                # embedding extraction
                text_embeds = clip_model.get_text_features(
                    input_ids=inputs.get('input_ids'),
                    attention_mask=inputs.get('attention_mask')
                )
                image_embeds = clip_model.get_image_features(
                    pixel_values=inputs.get('pixel_values')
                )
                
                return text_embeds.cpu().numpy(), image_embeds.cpu().numpy()
                
            except torch.cuda.OutOfMemoryError as e:
                print(f"[OOM] Out of Memory detected in the batch, emptying cache CUDA: {e}")
                # emptying  cache CUDA
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                
                # Fallback: process individually (slow but fallback)
                print(f"[OOM] Single-processing fallback for {len(texts)} elements")
                text_embeds_list = []
                image_embeds_list = []
                
                for i, (text, image) in enumerate(zip(texts, images)):
                    try:
                        single_inputs = clip_processor(
                            text=[text],
                            images=[image],
                            return_tensors="pt",
                            padding=True
                        )
                        single_inputs = {k: v.to(device) for k, v in single_inputs.items()}
                        
                        with torch.no_grad():
                            text_embed = clip_model.get_text_features(
                                input_ids=single_inputs.get('input_ids'),
                                attention_mask=single_inputs.get('attention_mask')
                            )
                            image_embed = clip_model.get_image_features(
                                pixel_values=single_inputs.get('pixel_values')
                            )
                            
                            text_embeds_list.append(text_embed.cpu().numpy())
                            image_embeds_list.append(image_embed.cpu().numpy())
                            
                    except Exception as single_e:
                        print(f"[OOM] Error in single element processing {i}: {single_e}")
                        # Add null embedding in case of error
                        text_embeds_list.append(np.zeros((1, 512)))
                        image_embeds_list.append(np.zeros((1, 512)))
                
                # Combine results
                text_embeds = np.vstack(text_embeds_list)
                image_embeds = np.vstack(image_embeds_list)
                
                return text_embeds, image_embeds
                
    except Exception as e:
        print(f"[ERROR] General error in batch embedding: {e}")
        return None, None

# ========== MAIN LOOP ==========

while True:
    print("TRANSFORMERS_CACHE set to:", os.environ.get("TRANSFORMERS_CACHE"))
    print(f"\n[INFO] Starting embedding extraction cycle - {datetime.now().isoformat()}")
    print(f"[INFO] Configuration: LIMIT_RECORDS={LIMIT_RECORDS}, BATCH_SIZE={BATCH_SIZE}, DEVICE={device}")
    
    last_guid = read_last_processed_guid()
    df_new = get_new_records(last_guid)

    if df_new.count() == 0:
        print("[INFO] No new records found. Waiting for next cycle...\n")
        time.sleep(SLEEP_SECONDS)
        continue

    columns = ["guid", "image_url", "title", "description", "type", "subject", "creator"]
    pandas_df = df_new.select(*columns).toPandas()
    print(f"[INFO] Records to be processed: {len(pandas_df)}")

    records = []
    # Buffer for batch accumulation
    batch_texts = []
    batch_images = []
    batch_guids = []
    batch_rows = []
    
    
    for idx, row in pandas_df.iterrows():
        guid = row["guid"]
        print(f"[BATCH] Processando record {idx+1}/{len(pandas_df)}: {guid}")
        
        
        text = preprocess_text(row)
        
        # Tokenize and truncate to 77 tokens (automatic cutting)
        tokens = clip_processor.tokenizer(
            text,
            truncation=True,
            max_length=77,
            return_tensors="pt"
        )
        # Reconstruct truncated text (optional, for log/debug)
        text_input = clip_processor.tokenizer.decode(
            tokens["input_ids"][0], skip_special_tokens=True
        )

        # Now text_input is safe to pass to the model (never more than 77 tokens)
        image_url = row["image_url"]
        if isinstance(image_url, list):
            image_url = image_url[0] if len(image_url) > 0 else None
        elif isinstance(image_url, str) and image_url.startswith("[") and image_url.endswith("]"):
            # if it is a string representing a list, converts it
            try:
                image_url_list = ast.literal_eval(image_url)
                if isinstance(image_url_list, list) and len(image_url_list) > 0:
                    image_url = image_url_list[0]
                else:
                    image_url = None
            except Exception:
                image_url = None
        
        image = fetch_image(image_url) if image_url else None

        # Accumulates batch records from BATCH_SIZE for embedding
        if image:  # Only if we have a valid image
            batch_texts.append(text_input)
            batch_images.append(image)
            batch_guids.append(guid)
            batch_rows.append(row)
            print(f"[BATCH] Added to batch: {len(batch_texts)}/{BATCH_SIZE}")
        else:
            # Process immediately if there is no image
            print(f"[BATCH] No image {guid}, immediate processing")
            embedding_status = "NO_IMAGE"
            emb_text = clip_model.get_text_features(**clip_processor(
                text=text_input, return_tensors="pt", padding=True)
            ).detach().cpu().numpy()[0].tolist()
            emb_image = None
            
            records.append({
                "guid": guid,
                "embedding_text": emb_text,
                "embedding_image": emb_image,
                "embedding_status": embedding_status,
            })

        # Process batch when it reaches BATCH_SIZE
        if len(batch_texts) >= BATCH_SIZE:
            print(f"[BATCH] Processing batches of {len(batch_texts)} elements...")
            
            # Send everything in one go to CLIP
            emb_texts, emb_images = get_embeddings_batch(batch_texts, batch_images)
            
            if emb_texts is not None and emb_images is not None:
                # Salva embedding_text, embedding_image in records[]
                for i, batch_guid in enumerate(batch_guids):
                    records.append({
                        "guid": batch_guid,
                        "embedding_text": emb_texts[i].tolist(),
                        "embedding_image": emb_images[i].tolist(),
                        "embedding_status": "OK",
                    })
                    print(f"[BATCH] Saved embedding for {batch_guid}")
            else:
                # Fallback in case of total batch error
                print(f"[BATCH] Error in batch, setting status FAILED for all items")
                for batch_guid in batch_guids:
                    records.append({
                        "guid": batch_guid,
                        "embedding_text": None,
                        "embedding_image": None,
                        "embedding_status": "FAILED",
                    })
            
            # Resets the batch buffer
            batch_texts = []
            batch_images = []
            batch_guids = []
            batch_rows = []
            print(f"[BATCH] Buffer reset")

    # Queued: batch remaining - process any last n < BATCH_SIZE elements left in the buffer
    if len(batch_texts) > 0:
        print(f"[BATCH] Processing remaining batch of {len(batch_texts)} elements...")
        
        # Send everything in one go to CLIP
        emb_texts, emb_images = get_embeddings_batch(batch_texts, batch_images)
        
        if emb_texts is not None and emb_images is not None:
            # Saves embedding_text, embedding_image in records[]
            for i, batch_guid in enumerate(batch_guids):
                records.append({
                    "guid": batch_guid,
                    "embedding_text": emb_texts[i].tolist(),
                    "embedding_image": emb_images[i].tolist(),
                    "embedding_status": "OK",
                })
                print(f"[BATCH] Saved final embedding for {batch_guid}")
        else:
            # Fallback in case of total batch error
            print(f"[BATCH] Error in final batch, setting status FAILED for all items")
            for batch_guid in batch_guids:
                records.append({
                    "guid": batch_guid,
                    "embedding_text": None,
                    "embedding_image": None,
                    "embedding_status": "FAILED",
                })

    print(f"[INFO] Completato processamento di {len(records)} record")

    if len(records) > 0:
        print(f"[QDRANT] Inizio upsert di {len(records)} record in Qdrant...")

        points = []
        for rec in records:
            if rec["embedding_status"] == "OK":
                point_id = sanitize_id(rec["guid"])

                # Prepare payload for Qdrant, without embedding
                payload = {
                    "guid": rec["guid"],
                    "status": "pending"
                }

                embedding_image = rec["embedding_image"]
                embedding_text = rec["embedding_text"]

                points.append(PointStruct(
                    id=point_id,
                    vector={
                        "image": embedding_image,
                        "combined": embedding_image + embedding_text  # Concatenates image and text embeddings
                    },
                    payload=payload  
                ))
            else:
                print(f"[SKIP] Skipping {rec['guid']} with status {rec['embedding_status']}")

        try:
            qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
            print(f"[QDRANT] Completated upsert of {len(points)} embeddings")

            last_guid_processed = pandas_df["guid"].iloc[-1]
            write_last_processed_guid(last_guid_processed)
            print(f"[STATE] Last GUID processed: {last_guid_processed}")

        except Exception as e:
            print(f"[QDRANT] Error during upsert in Qdrant: {e}")


        print("[INFO] Embedding extraction batch completated. Waiting...")
        print(f"[INFO] Next cycle in {SLEEP_SECONDS} seconds...")
        time.sleep(SLEEP_SECONDS)