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
import ast

from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct
from qdrant_client.http.models import PayloadSchemaType

import hashlib

# ========== CONFIGURAZIONE ==========
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
CLEANSED_PATH = "s3a://heritage/cleansed/europeana/"
EMBEDDING_PATH = "s3a://heritage/cleansed/embeddings/"
STATE_FILE_PATH = "s3a://heritage/cleansed/embedding_last_processed.txt"
CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"
SLEEP_SECONDS = 45

# ========== NUOVE CONFIGURAZIONI ==========
LIMIT_RECORDS = 112  # Limita i record per ciclo Spark (gestione RAM)
BATCH_SIZE = 16      # Batch size per embedding CLIP

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

# ========== INIZIALIZZA CLIP CON SUPPORTO GPU/CPU ==========
# Supporto automatico cuda/cpu
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"[DEVICE] Utilizzando dispositivo: {device}")

clip_model = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(device)
clip_processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)


# ========== INIZIALIZZA QDRANT ==========
# Configura Qdrant client (lo chiamiamo una volta sola)
qdrant = QdrantClient(host="qdrant", port=6333)

# Crea la collection se non esiste (solo all’avvio)
COLLECTION_NAME = "heritage_embeddings"
qdrant.recreate_collection(
    collection_name=COLLECTION_NAME,
    vectors_config={
        "image": VectorParams(size=512, distance=Distance.COSINE),         # solo immagine → per deduplicazione
        "combined": VectorParams(size=1024, distance=Distance.COSINE)      # immagine + testo → per raccomandazioni
    }
)
# Aggiungi payload index sul campo 'status' per filtrare punti pending/validated
try:
    qdrant.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="status",
        field_schema=PayloadSchemaType.Keyword
    )
    print("[QDRANT] Creato payload index sul campo 'status'")
except Exception as e:
    print(f"[QDRANT] Errore nella creazione payload index (forse esiste già): {e}")



# ========== FUNZIONI DI UTILITY ==========
def sanitize_id(guid):
    """
    Converte il GUID (che può essere un URL lungo) in una stringa compatibile con Qdrant
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
        print(f"[STATE] Ultimo guid processato: {guid}")
        return guid
    except Exception as e:
        print(f"[STATE] Nessun file di stato (embedding_last_processed.txt) trovato. Inizio dal primo record. ({e})")
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
    df = spark.read.format("delta").load(CLEANSED_PATH)
    if last_guid:
        df = df.filter(col("guid") > last_guid)
    # Limita i record a LIMIT_RECORDS per ciclo Spark (gestione RAM)
    df = df.orderBy(col("guid").asc()).limit(LIMIT_RECORDS)
    print(f"[SPARK] Limitando a {LIMIT_RECORDS} record per ciclo per gestione RAM")
    return df

def preprocess_text(row):
    # Ordine: title, subject, creator, type, description
    title = str(row["title"]) if row["title"] else ""
    subject = str(row["subject"]) if row["subject"] else ""
    creator = str(row["creator"]) if row["creator"] else ""
    type_ = str(row["type"]) if row["type"] else ""
    # Taglia description a max 150 caratteri per non saturare i token CLIP
    description = str(row["description"])[:150] if row["description"] else ""

    # Concatena nel nuovo ordine
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
        print(f"[ERROR] Eccezione nel download immagine: {url} ({e})")
        return None


def get_embeddings_batch(texts, images):
    """
    Nuova funzione per embedding batch con gestione OOM
    """
    try:
        # Prepara gli input per il batch
        inputs = clip_processor(
            text=texts,
            images=images,
            return_tensors="pt",
            padding=True
        )
        
        # Sposta gli input sul device (GPU/CPU)
        inputs = {k: v.to(device) for k, v in inputs.items()}
        
        with torch.no_grad():
            # Gestione OOM con try/except
            try:
                # Estrai embedding di testo e immagine
                text_embeds = clip_model.get_text_features(
                    input_ids=inputs.get('input_ids'),
                    attention_mask=inputs.get('attention_mask')
                )
                image_embeds = clip_model.get_image_features(
                    pixel_values=inputs.get('pixel_values')
                )
                
                return text_embeds.cpu().numpy(), image_embeds.cpu().numpy()
                
            except torch.cuda.OutOfMemoryError as e:
                print(f"[OOM] Out of Memory rilevato nel batch, svuotando cache CUDA: {e}")
                # Svuota la cache CUDA
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                
                # Fallback: processa singolarmente (lento ma sicuro)
                print(f"[OOM] Fallback a processamento singolo per {len(texts)} elementi")
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
                        print(f"[OOM] Errore nel processamento singolo elemento {i}: {single_e}")
                        # Aggiungi embedding nullo in caso di errore
                        text_embeds_list.append(np.zeros((1, 512)))
                        image_embeds_list.append(np.zeros((1, 512)))
                
                # Combina i risultati
                text_embeds = np.vstack(text_embeds_list)
                image_embeds = np.vstack(image_embeds_list)
                
                return text_embeds, image_embeds
                
    except Exception as e:
        print(f"[ERROR] Errore generale nell'embedding batch: {e}")
        return None, None

# ========== MAIN LOOP ==========

while True:
    print("TRANSFORMERS_CACHE set to:", os.environ.get("TRANSFORMERS_CACHE"))
    print(f"\n[INFO] Avvio ciclo estrazione embedding - {datetime.now().isoformat()}")
    print(f"[INFO] Configurazione: LIMIT_RECORDS={LIMIT_RECORDS}, BATCH_SIZE={BATCH_SIZE}, DEVICE={device}")
    
    last_guid = read_last_processed_guid()
    df_new = get_new_records(last_guid)

    if df_new.count() == 0:
        print("[INFO] Nessun nuovo record trovato. Attendo il prossimo ciclo...\n")
        time.sleep(SLEEP_SECONDS)
        continue

    columns = ["guid", "title", "description", "type", "subject", "creator", "isShownBy"]
    pandas_df = df_new.select(*columns).toPandas()
    print(f"[INFO] Record da processare: {len(pandas_df)}")

    records = []
    # Buffer per accumulo batch
    batch_texts = []
    batch_images = []
    batch_guids = []
    batch_rows = []
    
    # Mantieni loop for row in pandas_df.iterrows() come richiesto
    for idx, row in pandas_df.iterrows():
        guid = row["guid"]
        print(f"[BATCH] Processando record {idx+1}/{len(pandas_df)}: {guid}")
        
        # 1. Prepara il testo concatenato nel giusto ordine
        text = preprocess_text(row)
        
        # 2. Tokenizza e tronca a 77 token (taglio automatico)
        tokens = clip_processor.tokenizer(
            text,
            truncation=True,
            max_length=77,
            return_tensors="pt"
        )
        # Ricostruisci il testo troncato (opzionale, per log/debug)
        text_input = clip_processor.tokenizer.decode(
            tokens["input_ids"][0], skip_special_tokens=True
        )

        # 3. Ora text_input è sicuro da passare al modello (mai più di 77 token)
        image_url = row["isShownBy"]
        if isinstance(image_url, list):
            image_url = image_url[0] if len(image_url) > 0 else None
        elif isinstance(image_url, str) and image_url.startswith("[") and image_url.endswith("]"):
            # Se è una stringa che rappresenta una lista, la converto
            try:
                image_url_list = ast.literal_eval(image_url)
                if isinstance(image_url_list, list) and len(image_url_list) > 0:
                    image_url = image_url_list[0]
                else:
                    image_url = None
            except Exception:
                image_url = None
        
        image = fetch_image(image_url) if image_url else None

        # Accumula i record in batch da BATCH_SIZE per l'embedding
        if image:  # Solo se abbiamo un'immagine valida
            batch_texts.append(text_input)
            batch_images.append(image)
            batch_guids.append(guid)
            batch_rows.append(row)
            print(f"[BATCH] Aggiunto al batch: {len(batch_texts)}/{BATCH_SIZE}")
        else:
            # Processa immediatamente se non c'è immagine
            print(f"[BATCH] Nessuna immagine per {guid}, processamento immediato")
            embedding_status = "NO_IMAGE"
            emb_text = clip_model.get_text_features(**clip_processor(
                text=text_input, return_tensors="pt", padding=True)
            ).detach().cpu().numpy()[0].tolist()
            emb_image = None
            
            records.append({
                "id_object": guid,
                "embedding_text": emb_text,
                "embedding_image": emb_image,
                "embedding_status": embedding_status,
            })

        # Processa batch quando raggiunge BATCH_SIZE
        if len(batch_texts) >= BATCH_SIZE:
            print(f"[BATCH] Processando batch di {len(batch_texts)} elementi...")
            
            # Manda tutto in un colpo a CLIP
            emb_texts, emb_images = get_embeddings_batch(batch_texts, batch_images)
            
            if emb_texts is not None and emb_images is not None:
                # Salva embedding_text, embedding_image in records[]
                for i, batch_guid in enumerate(batch_guids):
                    records.append({
                        "id_object": batch_guid,
                        "embedding_text": emb_texts[i].tolist(),
                        "embedding_image": emb_images[i].tolist(),
                        "embedding_status": "OK",
                    })
                    print(f"[BATCH] Salvato embedding per {batch_guid}")
            else:
                # Fallback in caso di errore totale del batch
                print(f"[BATCH] Errore nel batch, impostando status FAILED per tutti gli elementi")
                for batch_guid in batch_guids:
                    records.append({
                        "id_object": batch_guid,
                        "embedding_text": None,
                        "embedding_image": None,
                        "embedding_status": "FAILED",
                    })
            
            # Resetta i buffer
            batch_texts = []
            batch_images = []
            batch_guids = []
            batch_rows = []
            print(f"[BATCH] Buffer resettato")

    # In coda: batch rimanente - elabora gli eventuali ultimi n < BATCH_SIZE elementi rimasti nel buffer
    if len(batch_texts) > 0:
        print(f"[BATCH] Processando batch rimanente di {len(batch_texts)} elementi...")
        
        # Manda tutto in un colpo a CLIP
        emb_texts, emb_images = get_embeddings_batch(batch_texts, batch_images)
        
        if emb_texts is not None and emb_images is not None:
            # Salva embedding_text, embedding_image in records[]
            for i, batch_guid in enumerate(batch_guids):
                records.append({
                    "id_object": batch_guid,
                    "embedding_text": emb_texts[i].tolist(),
                    "embedding_image": emb_images[i].tolist(),
                    "embedding_status": "OK",
                })
                print(f"[BATCH] Salvato embedding finale per {batch_guid}")
        else:
            # Fallback in caso di errore totale del batch
            print(f"[BATCH] Errore nel batch finale, impostando status FAILED per tutti gli elementi")
            for batch_guid in batch_guids:
                records.append({
                    "id_object": batch_guid,
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
                point_id = sanitize_id(rec["id_object"])

                # Payload ridotto, senza embedding_text
                payload = {
                    "id_object": rec["id_object"],
                    "status": "pending"
                }

                embedding_image = rec["embedding_image"]
                embedding_text = rec["embedding_text"]

                points.append(PointStruct(
                    id=point_id,
                    vector={
                        "image": embedding_image,
                        "combined": embedding_image + embedding_text  # concatenazione
                    },
                    payload=payload  
                ))
            else:
                print(f"[SKIP] Skipping {rec['id_object']} with status {rec['embedding_status']}")

        try:
            qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
            print(f"[QDRANT] Completato upsert di {len(points)} embeddings")

            last_guid_processed = pandas_df["guid"].iloc[-1]
            write_last_processed_guid(last_guid_processed)
            print(f"[STATE] Ultimo GUID processato: {last_guid_processed}")

        except Exception as e:
            print(f"[QDRANT] Errore durante upsert in Qdrant: {e}")


        print("[INFO] Embedding extraction batch completato. Attendo...")
        print(f"[INFO] Prossimo ciclo tra {SLEEP_SECONDS} secondi...")
        time.sleep(SLEEP_SECONDS)