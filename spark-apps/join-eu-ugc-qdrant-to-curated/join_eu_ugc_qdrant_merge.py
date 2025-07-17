# every comments!
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, first, when, isnan, isnull
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import time
from delta.tables import DeltaTable
import pandas as pd # Import pandas here as it's used in your function

# =================== CONFIG ===================
# MinIO paths
UGC_PATH = "s3a://heritage/cleansed/user_generated/"
EUROPEANA_PATH = "s3a://heritage/cleansed/europeana/"
CURATED_PATH = "s3a://heritage/curated/join_metadata_deduplicated/"

# Qdrant config
QDRANT_HOST = "qdrant"
QDRANT_PORT = 6333
QDRANT_COLLECTION = "heritage_embeddings"

# Reload intervals (in minutes)
RELOAD_EUROPEANA_MIN = 1
RELOAD_QDRANT_MIN = 1

# =============================================

def get_all_validated_canonical_groups_from_qdrant():
    """
    Query Qdrant per tutti i punti con status = 'validated'.
    Restituisce un dizionario: canonical_id -> lista di tutti i guid del gruppo.
    Questo ci permette di recuperare tutti i duplicati per ogni canonical_id.
    """
    print("[DEBUG] Connessione a Qdrant e recupero punti 'validated'...")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=20)

    results = []
    offset = None

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

    points_data = [point.payload for point in results if point.payload]
    
    # Handle case where no payloads are found or required columns are missing
    if not points_data:
        print("[DEBUG WARN] Nessun payload trovato in Qdrant o risultati vuoti.")
        return {}
    
    df = pd.DataFrame(points_data)

    if 'canonical_id' not in df.columns or 'guid' not in df.columns:
        print("[DEBUG WARN] Campi canonical_id o guid non trovati nei payload Qdrant. Colonne disponibili: ", df.columns.tolist())
        return {}

    # Raggruppa per canonical_id e ottieni tutti i guid per ogni gruppo
    canonical_groups = {}
    for canonical_id, group_df in df.groupby('canonical_id'):
        guid_list = group_df['guid'].tolist()
        canonical_groups[canonical_id] = guid_list
        # print(f"[DEBUG] Canonical ID {canonical_id}: {len(guid_list)} guid ({guid_list})") # Only for very detailed debug
    
    # Add a check if canonical_groups is empty here too
    if not canonical_groups:
        print("[DEBUG WARN] Nessun gruppo canonical_id formato dai dati Qdrant.")

    print(f"[DEBUG] Totale gruppi canonical_id: {len(canonical_groups)}")
    return canonical_groups

def get_representative_guids_from_canonical_groups(canonical_groups):
    """
    Da ogni gruppo canonical_id, sceglie un guid rappresentativo.
    Restituisce un dizionario: representative_guid -> lista di tutti i guid del gruppo.
    """
    representative_mapping = {}
    
    for canonical_id, guid_list in canonical_groups.items():
        if guid_list: # Ensure guid_list is not empty
            representative_guid = guid_list[0] # Sceglie il primo guid come rappresentativo
            representative_mapping[representative_guid] = guid_list
            # print(f"[DEBUG] Canonical {canonical_id}: rappresentativo={representative_guid}, gruppo={guid_list}") # Only for very detailed debug
        else:
            print(f"[DEBUG WARN] Gruppo canonical_id {canonical_id} ha una lista guid vuota.")
    
    return representative_mapping

def get_latest_processed_timestamp(spark_session):
    """
    Recupera il timestamp massimo già processato dal layer curated.
    """
    try:
        if DeltaTable.isDeltaTable(spark_session, CURATED_PATH):
            df_curated = spark_session.read.format("delta").load(CURATED_PATH)
            max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
            print(f"[DEBUG] Timestamp massimo trovato nel layer curated: {max_ts}")
            return max_ts
        else:
            print("[DEBUG] La tabella Delta in CURATED_PATH non esiste ancora.")
            return None
    except Exception as e:
        # This catch is broad, consider refining if specific errors are known
        print(f"[DEBUG] Errore nel recupero del timestamp massimo: {str(e)}. La tabella potrebbe essere vuota o non esistere o non avere la colonna 'timestamp'.")
        return None

def read_latest_delta_table(spark_session, path):
    return spark_session.read.format("delta").load(path)

# ========== SPARK SESSION ==========
builder = SparkSession.builder \
    .appName("Complete_Join_EU_UGC_Qdrant_AllComments") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ========== INITIALIZATION ==========
last_europeana_reload = 0
last_qdrant_reload = 0
europeana_df = None
canonical_groups = None
representative_mapping = None

print("[DEBUG] Job avviato: join completo con tutti i commenti + filtro Qdrant")

while True:
    now = time.time()

    # Reload Europeana if necessary
    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        print("[DEBUG] Ricarico metadati Europeana da Delta...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        europeana_df.cache() # Cache for multiple uses
        print(f"[DEBUG] Europeana DF righe: {europeana_df.count()}")
        last_europeana_reload = now

    # Reload validated IDs from Qdrant if necessary
    if canonical_groups is None or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        canonical_groups = get_all_validated_canonical_groups_from_qdrant()
        representative_mapping = get_representative_guids_from_canonical_groups(canonical_groups)
        last_qdrant_reload = now

    if not canonical_groups:
        print("[DEBUG] Nessun gruppo canonical_id validato da Qdrant. Nessun dato da processare. Attendo...")
        time.sleep(60)
        continue

    # Trova l'ultimo timestamp processato dal layer curated
    latest_ts = get_latest_processed_timestamp(spark)
    print(f"[DEBUG] Ultimo timestamp processato in curated: {latest_ts}")

    # Leggi tutte le annotazioni da UGC
    ugc_df = spark.read.format("delta").load(UGC_PATH)
    ugc_df.cache() # Cache for multiple uses
    print(f"[DEBUG] UGC DF righe totali: {ugc_df.count()}")
    
    # Per elaborazione incrementale, filtra solo annotazioni nuove se necessario
    if latest_ts:
        ugc_df_incremental = ugc_df.filter(col("timestamp") > lit(latest_ts))
        print(f"[DEBUG] UGC totali: {ugc_df.count()}, UGC nuovi (timestamp > {latest_ts}): {ugc_df_incremental.count()}")
        
        if ugc_df_incremental.count() == 0:
            print("[DEBUG] Nessuna nuova annotazione UGC trovata. Attendo...")
            ugc_df.unpersist() # Unpersist if not used further in this iteration
            europeana_df.unpersist() # Unpersist if not used further in this iteration
            time.sleep(60)
            continue
    else:
        ugc_df_incremental = ugc_df
        print(f"[DEBUG] Primo run: elaboro tutti i dati UGC ({ugc_df.count()} righe)")

    # STRATEGIA: Prima fai il join completo tra Europeana e UGC, poi filtra per Qdrant
    print("[DEBUG] Eseguo LEFT JOIN completo tra Europeana e UGC...")
    
    # Join completo: tutti i dati Europeana + eventuali annotazioni UGC
    complete_joined_df = europeana_df.join(ugc_df, on="guid", how="left") \
                                     .withColumn("joined_at", current_timestamp())
    complete_joined_df.cache() # Cache for subsequent operations
    current_count = complete_joined_df.count()
    print(f"[DEBUG] Risultato join completo Europeana + UGC: {current_count} righe")
    
    if current_count == 0:
        print("[DEBUG] Risultato join Europeana + UGC è vuoto. Attendo...")
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # Ora applica il filtro Qdrant: mantieni solo i guid che sono presenti in Qdrant
    # Crea una lista di tutti i guid validati (tutti i guid di tutti i gruppi)
    all_validated_guids = []
    for guid_list in canonical_groups.values():
        all_validated_guids.extend(guid_list)
    
    all_validated_guids = list(set(all_validated_guids))  # Rimuovi duplicati
    print(f"[DEBUG] Totale guid validati da Qdrant: {len(all_validated_guids)}")

    # Filtra il join completo per includere solo i guid presenti in Qdrant
    qdrant_filtered_df = complete_joined_df.filter(col("guid").isin(all_validated_guids))
    qdrant_filtered_df.cache()
    current_count = qdrant_filtered_df.count()
    print(f"[DEBUG] Righe dopo filtro Qdrant: {current_count}")

    if current_count == 0:
        print("[DEBUG] Nessuna riga dopo il filtro Qdrant. Attendo...")
        qdrant_filtered_df.unpersist()
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # Ora applica il filtro temporale per elaborazione incrementale
    if latest_ts:
        # Mantieni: 1) righe senza annotazioni (timestamp is null)
        #           2) righe con annotazioni nuove (timestamp > latest_ts)
        final_df = qdrant_filtered_df.filter(
            col("timestamp").isNull() | (col("timestamp") > lit(latest_ts))
        )
    else:
        final_df = qdrant_filtered_df
    
    final_df.cache()
    current_count = final_df.count()
    print(f"[DEBUG] Righe finali dopo filtro temporale: {current_count}")

    if current_count == 0:
        print("[DEBUG] Nessuna riga dopo il filtro temporale finale. Attendo...")
        final_df.unpersist()
        qdrant_filtered_df.unpersist()
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # AGGREGAZIONE PER CANONICAL_ID:
    print("[DEBUG] Inizia aggregazione per canonical_id...")
    
    # Creiamo un mapping da guid a canonical_id per poter fare il raggruppamento
    guid_to_canonical = {}
    for canonical_id, guid_list in canonical_groups.items():
        for guid in guid_list:
            guid_to_canonical[guid] = canonical_id
    
    # Aggiungi colonna canonical_id al DataFrame
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    def get_canonical_id_udf(guid): # Renamed to avoid conflict
        return guid_to_canonical.get(guid, guid) # Se non trovato, usa lo stesso guid
    
    canonical_udf = udf(get_canonical_id_udf, StringType())
    
    final_df_with_canonical = final_df.withColumn("canonical_id", canonical_udf(col("guid")))
    final_df_with_canonical.cache()
    print(f"[DEBUG] Righe dopo aggiunta canonical_id: {final_df_with_canonical.count()}")

    # Separare righe con e senza commenti
    with_comments = final_df_with_canonical.filter(col("timestamp").isNotNull())
    without_comments = final_df_with_canonical.filter(col("timestamp").isNull())
    
    print(f"[DEBUG] Righe con commenti (before canonical processing): {with_comments.count()}")
    print(f"[DEBUG] Righe senza commenti (before canonical processing): {without_comments.count()}")
    
    # Per le righe senza commenti, prendiamo solo il guid rappresentativo per ogni canonical_id
    # Ensure representative_mapping is not empty before creating the list
    if representative_mapping:
        representative_guids_list = [rep_guid for rep_guid in representative_mapping.keys()]
        representative_without_comments = without_comments.filter(col("guid").isin(representative_guids_list))
    else:
        representative_without_comments = spark.createDataFrame([], without_comments.schema) # Empty DF with same schema
        print("[DEBUG WARN] representative_mapping è vuoto, nessuna riga rappresentativa senza commenti verrà creata.")

    representative_without_comments.cache()
    print(f"[DEBUG] Righe rappresentative senza commenti (after filter): {representative_without_comments.count()}")
    
    # Per le righe con commenti, aggiorniamo il guid al rappresentativo mantenendo tutti i commenti
    if with_comments.count() > 0:
        # Mapping da guid a rappresentativo
        guid_to_representative = {}
        for rep_guid, guid_list in representative_mapping.items():
            for guid in guid_list:
                guid_to_representative[guid] = rep_guid
        
        def get_representative_guid_udf(guid): # Renamed to avoid conflict
            return guid_to_representative.get(guid, guid)
        
        representative_udf = udf(get_representative_guid_udf, StringType())
        
        # Aggiorna il guid al rappresentativo per i commenti
        with_comments_representative = with_comments.withColumn(
            "guid", representative_udf(col("guid"))
        )
        
        # Unisci con i metadati Europeana del rappresentativo
        # Prima ottieni solo i metadati Europeana per i guid rappresentativi
        if representative_mapping: # Ensure representative_mapping is not empty
            europeana_representative = europeana_df.filter(
                col("guid").isin([rep_guid for rep_guid in representative_mapping.keys()])
            )
        else:
             europeana_representative = spark.createDataFrame([], europeana_df.schema) # Empty DF with same schema
             print("[DEBUG WARN] representative_mapping è vuoto, nessuna metadati Europeana rappresentativa verrà unita.")


        # Fai il join per ottenere i metadati corretti
        # Select columns to avoid ambiguity if 'guid' is in both
        europeana_columns = [c for c in europeana_df.columns if c != "guid"] # All Europeana cols except guid
        ugc_columns_for_join = [c for c in with_comments_representative.columns if c not in europeana_columns and c != "guid"]
        
        with_comments_final = with_comments_representative.select("guid", *ugc_columns_for_join).join(
            europeana_representative.select("guid", *europeana_columns), on="guid", how="left"
        ).withColumn("joined_at", current_timestamp())
        
        with_comments_final.cache()
        print(f"[DEBUG] Righe con commenti dopo join con rappresentativo Europeana: {with_comments_final.count()}")

        # Combina righe con e senza commenti
        # Ensure schemas are aligned before union
        # Get common columns
        common_cols = list(set(representative_without_comments.columns) & set(with_comments_final.columns))
        final_result_union_aligned = representative_without_comments.select(common_cols).unionByName(with_comments_final.select(common_cols))
        final_result_union_aligned.cache()
        print(f"[DEBUG] Risultato unione di righe con e senza commenti: {final_result_union_aligned.count()}")
        
        final_result = final_result_union_aligned
    else:
        print("[DEBUG] Nessun commento da processare con logica rappresentativa.")
        final_result = representative_without_comments
    
    final_result.cache()
    print(f"[DEBUG] Risultato finale prima della deduplicazione: {final_result.count()}")

    # DEDUPLICAZIONE finale
    final_result_deduplicated = final_result.dropDuplicates(["guid", "user_id", "timestamp"])
    final_result_deduplicated.cache() # Cache before final count and write
    
    row_count = final_result_deduplicated.count()
    print(f"[DEBUG] Risultato finale dopo deduplicazione: {row_count}")

    print(f"[METRICHE] Oggetti Europeana totali: {europeana_df.count()}")
    print(f"[METRICHE] Oggetti Europeana validati (presenti in Qdrant): {len(all_validated_guids)}")
    print(f"[METRICHE] Gruppi canonical_id: {len(canonical_groups)}")
    print(f"[METRICHE] Righe finali da scrivere: {row_count}")
    
    # Unpersist all cached DFs that are no longer needed
    europeana_df.unpersist()
    ugc_df.unpersist()
    complete_joined_df.unpersist()
    qdrant_filtered_df.unpersist()
    final_df.unpersist()
    final_df_with_canonical.unpersist()
    representative_without_comments.unpersist()
    if 'with_comments_final' in locals() and with_comments_final.is_cached:
        with_comments_final.unpersist()
    if 'final_result_union_aligned' in locals() and final_result_union_aligned.is_cached:
        final_result_union_aligned.unpersist()
    final_result.unpersist() # Unpersist before final_result_deduplicated is processed by write
    # final_result_deduplicated will be unpersisted by Spark after write or when driver exits

    # Gestione della scrittura nel layer curato con MERGE
    print(f"[DEBUG] Elaborazione completata. Righe da scrivere tramite MERGE: {row_count}")

    if row_count > 0:
        # Verifica se la tabella Delta esiste, altrimenti la crea
        if not DeltaTable.isDeltaTable(spark, CURATED_PATH):
            print(f"[DEBUG] La tabella Delta in '{CURATED_PATH}' non esiste. Creazione iniziale della tabella.")
            final_result_deduplicated.write.format("delta").mode("overwrite").save(CURATED_PATH)
            print(f"[DEBUG] Tabella Delta creata con overwrite in '{CURATED_PATH}'.")
        else:
            deltaTable = DeltaTable.forPath(spark, CURATED_PATH)

            print("[DEBUG] Esegui MERGE nel layer curated...")
            deltaTable.alias("target") \
              .merge(
                final_result_deduplicated.alias("source"),
                "target.guid = source.guid AND " +
                "((target.timestamp IS NULL AND source.timestamp IS NULL) OR " +
                " (target.timestamp = source.timestamp)) AND " +
                "((target.user_id IS NULL AND source.user_id IS NULL) OR " +
                " (target.user_id = source.user_id))"
              ) \
              .whenNotMatchedInsertAll() \
              .whenMatchedUpdateAll() \
              .execute()
            print("[DEBUG] Scrittura completata nel layer curated tramite MERGE.")
    else:
        print("[DEBUG] Nessuna riga da scrivere tramite MERGE.")

    print("[DEBUG] Attendo 70 secondi prima del prossimo ciclo...")
    time.sleep(70)