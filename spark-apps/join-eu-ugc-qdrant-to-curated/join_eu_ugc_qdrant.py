# join_eu_ugc_qdrant.py
# ------------------------------------------------------------
# Join Europeana + UGC filtrato con validated_ids da Qdrant
# Ora con normalizzazione dei GUID
# ------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, max as spark_max, first, udf
)
from pyspark.sql.types import StringType
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
from urllib.parse import urlparse, parse_qs, urlunparse, unquote, urlencode
import time
import os

# =================== CONFIG ===================
UGC_PATH       = "s3a://heritage/cleansed/user_generated/"
EUROPEANA_PATH = "s3a://heritage/cleansed/europeana/"
CURATED_PATH   = "s3a://heritage/curated/join_metadata_deduplicated/"

QDRANT_HOST       = "qdrant"
QDRANT_PORT       = 6333
QDRANT_COLLECTION = "heritage_embeddings"

RELOAD_EUROPEANA_MIN = 5
RELOAD_QDRANT_MIN    = 5
# ==============================================

# ---------- GUID normalisation -------------------------------------------------
TRACKING_PREFIXES = ("utm_", "api")

def normalize_guid(url: str) -> str:
    """
    Rende canonico l'URL:
      – rimuove i parametri query che iniziano con utm_ o api
      – decodifica URL-encoded
      – lowercase sul dominio
      – elimina lo slash finale
    """
    if url is None:
        return None
    url = unquote(url.strip())
    p   = urlparse(url)
    qs  = {k: v for k, v in parse_qs(p.query, keep_blank_values=True).items()
           if not k.startswith(TRACKING_PREFIXES)}
    return urlunparse((
        p.scheme,
        p.netloc.lower(),
        p.path.rstrip("/"),
        "",                        # params
        urlencode(qs, doseq=True), # query
        ""                         # fragment
    ))

normalize_udf = udf(normalize_guid, StringType())
# -------------------------------------------------------------------------------


def get_validated_ids_from_qdrant():
    """
    Query Qdrant per tutti i punti con status='validated'.
    Ritorna un set di id_object deduplicati e normalizzati.
    """
    print("[DEBUG] Connessione a Qdrant e recupero punti 'validated'...")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    results, offset = [], None
    while True:
        batch, offset = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[FieldCondition(key="status", match=MatchValue(value="validated"))]
            ),
            limit=1000,
            with_payload=True
        )
        results.extend(batch)
        if offset is None:
            break

    print(f"[DEBUG] Trovati {len(results)} punti con status=validated")

    import pandas as pd
    points_data = [p.payload for p in results if p.payload]
    df = pd.DataFrame(points_data)

    if 'canonical_id' not in df.columns or 'id_object' not in df.columns:
        print("[DEBUG WARN] Campi canonical_id o id_object non trovati nei payload Qdrant.")
        return set()

    validated_ids_raw = (
        df.groupby("canonical_id")
          .first()["id_object"]
          .astype(str)
          .tolist()
    )
    # normalizza subito
    validated_ids = set(map(normalize_guid, validated_ids_raw))
    print(f"[DEBUG] Totale id_object deduplicati (uno per canonical_id): {len(validated_ids)}")
    print("[DEBUG] Esempi di id_object normalizzati (primi 10):")
    for id_ in list(validated_ids)[:10]:
        print(f" - {id_}")
    return validated_ids


def get_latest_processed_timestamp(spark):
    """
    Recupera il timestamp massimo già processato dal layer curated.
    """
    try:
        df_curated = spark.read.format("delta").load(CURATED_PATH)
        max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
        print(f"[DEBUG] Timestamp massimo trovato nel layer curated: {max_ts}")
        return max_ts
    except Exception as e:
        print(f"[DEBUG] Nessun timestamp trovato: la tabella curated potrebbe non esistere ancora. Dettaglio: {e}")
        return None


def read_latest_delta_table(spark, path):
    return spark.read.format("delta").load(path)


# ========== SPARK SESSION ==========
builder = (
    SparkSession.builder
      .appName("Join_EU_UGC_Qdrant_Curated")
      .config("spark.hadoop.fs.s3a.access.key",     os.getenv("AWS_ACCESS_KEY_ID",     "minio"))
      .config("spark.hadoop.fs.s3a.secret.key",     os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
      .config("spark.hadoop.fs.s3a.endpoint",       os.getenv("AWS_ENDPOINT", "http://minio:9000"))
      .config("spark.hadoop.fs.s3a.impl",           "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.sql.extensions",               "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ========== INITIALIZATION ==========
last_europeana_reload = 0
last_qdrant_reload    = 0
europeana_df          = None
validated_ids_bcast   = None   # broadcast variable

print("[DEBUG] Job avviato: join con filtro Qdrant + Delta (GUID normalizzati)")

while True:
    now = time.time()

    # --- Europaena reload ------------------------------------------------------
    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        print("[DEBUG] Ricarico metadati Europeana da Delta...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH) \
                          .withColumn("guid_norm", normalize_udf(col("guid")))
        last_europeana_reload = now

    # --- Validated IDs reload --------------------------------------------------
    if (validated_ids_bcast is None) or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        validated_ids = get_validated_ids_from_qdrant()
        validated_ids_bcast = spark.sparkContext.broadcast(list(validated_ids))
        last_qdrant_reload = now

    # --- Filtra Europeana sui GUID norm ----------------------------------------
    filtered_europeana_df = europeana_df.filter(col("guid_norm").isin(validated_ids_bcast.value))

    print("[DEBUG] Esempio Europeana filtrata (guid_norm, title):")
    cols_to_show = ["guid_norm"] + (["title"] if "title" in filtered_europeana_df.columns else [])
    filtered_europeana_df.select(*cols_to_show).show(5, truncate=False)

    print(f"[DEBUG] Europeana count totale: {europeana_df.count()}")
    print(f"[DEBUG] Europeana filtrata: {filtered_europeana_df.count()}")

    # --- Recupera ultimo timestamp dal curated ---------------------------------
    latest_ts = get_latest_processed_timestamp(spark)

    # --- Carica UGC ------------------------------------------------------------
    ugc_df = spark.read.format("delta").load(UGC_PATH)
    if latest_ts:
        ugc_df = ugc_df.filter(col("timestamp") > lit(latest_ts))
    ugc_df = (
        ugc_df.withColumnRenamed("object_id", "guid")
              .withColumn("guid_norm", normalize_udf(col("guid")))
    )

    if ugc_df.rdd.isEmpty():
        print("[DEBUG] Nessuna nuova annotazione. Attendo...")
        time.sleep(60)
        continue

    print("[DEBUG] Esempio UGC (guid_norm, timestamp):")
    ugc_df.select("guid_norm", "timestamp").show(5, truncate=False)

    # --- Intersezione rapida ----------------------------------------------------
    ugc_ids        = set(r["guid_norm"] for r in ugc_df.select("guid_norm").distinct().collect())
    europeana_ids  = set(r["guid_norm"] for r in filtered_europeana_df.select("guid_norm").distinct().collect())
    intersection   = ugc_ids & europeana_ids
    print(f"[DEBUG] Oggetti in comune (guid_norm): {len(intersection)}")

    if not intersection:
        print("[DEBUG] Nessuna annotazione corrisponde. Attendo...")
        time.sleep(60)
        continue

    # --- Join ------------------------------------------------------------------
    print("[DEBUG] Eseguo join su guid_norm...")
    joined_df = (
        ugc_df.join(filtered_europeana_df, on="guid_norm", how="inner")
              .withColumn("joined_at", current_timestamp())
    )

    row_count = joined_df.count()
    print(f"[DEBUG] Join completato. Righe da scrivere: {row_count}")

    if row_count:
        joined_df.write.format("delta").mode("append").save(CURATED_PATH)
        print("[DEBUG] Scrittura completata nel layer curated.")

    print("[DEBUG] Attendo 60 secondi prima del prossimo ciclo...")
    time.sleep(60)
