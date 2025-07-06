import os
import time
import io
import requests
from datetime import datetime

import torch
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, StringType, StructType, StructField
from pyspark.sql.functions import col

from transformers import CLIPProcessor, CLIPModel

# ========== CONFIGURAZIONE ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
CURATED_PATH = "s3a://heritage/curated/join_metadata/"
EMBEDDING_PATH = "s3a://heritage/curated/embeddings/"
STATE_FILE_PATH = "s3a://heritage/curated/embedding_last_processed.txt"
CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"
SLEEP_SECONDS = 60

# ========== INIZIALIZZA SPARK ==========
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

# ========== INIZIALIZZA CLIP ==========
device = "cpu"
clip_model = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(device)
clip_processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)

# ========== FUNZIONI DI UTILITY ==========

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
        print(f"[STATE] Ultimo guid processato: {guid}")
        return guid
    except Exception as e:
        print(f"[STATE] Nessun file di stato trovato. Inizio dal primo record. ({e})")
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
    print(f"[STATE] File di stato aggiornato con guid: {guid}")

def get_new_records(last_guid):
    df = spark.read.format("delta").load(CURATED_PATH)
    if last_guid:
        df = df.filter(col("guid") > last_guid)
    return df.orderBy(col("guid").asc())

def preprocess_text(row):
    fields = [
        row["tags"] if row["tags"] else [],
        row["title"] if row["title"] else "",
        row["description"] if row["description"] else "",
        row["type"] if row["type"] else "",
        row["subject"] if row["subject"] else "",
        row["creator"] if row["creator"] else "",
    ]
    tags_str = ", ".join(fields[0]) if isinstance(fields[0], list) else str(fields[0])
    text = " ".join([tags_str, *fields[1:]])
    return text.strip()

def fetch_image(url):
    from PIL import Image
    try:
        resp = requests.get(url, timeout=8)
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        return img
    except Exception as e:
        print(f"[IMAGE] Fallito il download dell'immagine: {url} ({e})")
        return None

def get_embeddings(texts, images):
    inputs = clip_processor(
        text=texts,
        images=images,
        return_tensors="pt",
        padding=True
    )
    with torch.no_grad():
        text_embeds = clip_model.get_text_features(**{k: v.to(device) for k, v in inputs.items() if k.startswith('input_ids') or k.startswith('attention_mask')})
        image_embeds = clip_model.get_image_features(**{k: v.to(device) for k, v in inputs.items() if k.startswith('pixel_values')})
    return text_embeds.cpu().numpy(), image_embeds.cpu().numpy()

# ========== DELTA LAKE MERGE UTILITY ==========
def merge_embeddings(df_out, path):
    from delta.tables import DeltaTable

    # Crea la tabella Delta se non esiste
    if not DeltaTable.isDeltaTable(spark, path):
        df_out.write.format("delta").mode("overwrite").save(path)
        print("[DELTA] Creata nuova DeltaTable embeddings (overwrite primo batch).")
        return

    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.alias("target").merge(
        df_out.alias("source"),
        "target.id_object = source.id_object"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[DELTA] Merge completato: upsert su id_object.")

# ========== MAIN LOOP ==========

while True:
    print(f"\n[INFO] Avvio ciclo estrazione embedding - {datetime.now().isoformat()}")
    last_guid = read_last_processed_guid()
    df_new = get_new_records(last_guid)

    if df_new.count() == 0:
        print("[INFO] Nessun nuovo record trovato. Attendo il prossimo ciclo...\n")
        time.sleep(SLEEP_SECONDS)
        continue

    columns = ["guid", "tags", "title", "description", "type", "subject", "creator", "IsShownBy"]
    pandas_df = df_new.select(*columns).toPandas()
    print(f"[INFO] Record da processare: {len(pandas_df)}")

    records = []
    for idx, row in pandas_df.iterrows():
        guid = row["guid"]
        text = preprocess_text(row)
        image_url = row["IsShownBy"]

        text_input = [text]
        image = fetch_image(image_url) if image_url else None
        if not image:
            embedding_status = "NO_IMAGE"
            emb_text = clip_model.get_text_features(**clip_processor(text=text_input, return_tensors="pt", padding=True)).detach().cpu().numpy()[0].tolist()
            emb_image = None
        else:
            try:
                emb_text, emb_image = get_embeddings(text_input, [image])
                embedding_status = "OK"
                emb_text = emb_text[0].tolist()
                emb_image = emb_image[0].tolist()
            except Exception as e:
                print(f"[ERROR] Errore nell'embedding CLIP per guid {guid}: {e}")
                embedding_status = "FAILED"
                emb_text = None
                emb_image = None

        records.append({
            "id_object": guid,
            "embedding_text": emb_text,
            "embedding_image": emb_image,
            "embedding_status": embedding_status,
        })

    output_schema = StructType([
        StructField("id_object", StringType(), False),
        StructField("embedding_text", ArrayType(FloatType()), True),
        StructField("embedding_image", ArrayType(FloatType()), True),
        StructField("embedding_status", StringType(), False),
    ])
    df_out = spark.createDataFrame(pd.DataFrame(records), schema=output_schema)

    # ====== MERGE/UPSERT IN DELTATABLE ======
    if df_out.count() > 0:
        try:
            merge_embeddings(df_out, EMBEDDING_PATH)
        except Exception as e:
            print(f"[ERROR] Scrittura DeltaTable con merge fallita: {e}")

        last_guid_processed = pandas_df["guid"].iloc[-1]
        write_last_processed_guid(last_guid_processed)

    print("[INFO] Embedding extraction batch completato. Attendo...")
    time.sleep(SLEEP_SECONDS)
