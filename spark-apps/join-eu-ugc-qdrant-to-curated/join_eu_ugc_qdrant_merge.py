from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max, first, when, isnan, isnull
from delta import configure_spark_with_delta_pip
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import time
from delta.tables import DeltaTable
import pandas as pd 

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
    Qdrant query for all points with status = ‘validated’.
    Returns a dictionary: canonical_id -> list of all guids in the group.
    This allows us to retrieve all duplicates for each canonical_id.
    """
    print("[DEBUG] Connection to Qdrant and recovery of ‘validated’ points ...")
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

    print(f"[DEBUG] Founded {len(results)} points with status=validated")

    points_data = [point.payload for point in results if point.payload]
    
    # Handle case where no payloads are found or required columns are missing
    if not points_data:
        print("[DEBUG WARN] No payload found in Qdrant or empty results.")
        return {}
    
    df = pd.DataFrame(points_data)

    if 'canonical_id' not in df.columns or 'guid' not in df.columns:
        print("[DEBUG WARN] canonical_id o guid not founded in payload Qdrant. Available columns: ", df.columns.tolist())
        return {}

    # Group by canonical_id and create a dictionary of guid lists
    canonical_groups = {}
    for canonical_id, group_df in df.groupby('canonical_id'):
        guid_list = group_df['guid'].tolist()
        canonical_groups[canonical_id] = guid_list
        # print(f"[DEBUG] Canonical ID {canonical_id}: {len(guid_list)} guid ({guid_list})") # Only for very detailed debug
    
    # Add a check if canonical_groups is empty here too
    if not canonical_groups:
        print("[DEBUG WARN] No canonical_id group found in Qdrant data.")

    print(f"[DEBUG] Total canonical_id groups: {len(canonical_groups)}")
    return canonical_groups

def get_representative_guids_from_canonical_groups(canonical_groups):
    """
    From each canonical_id group, selects a representative guid.
    Returns a dictionary: representative_guid -> list of all guids in the group.
    """
    representative_mapping = {}
    
    for canonical_id, guid_list in canonical_groups.items():
        if guid_list: # Ensure guid_list is not empty
            representative_guid = guid_list[0] # Select the first guid as representative
            representative_mapping[representative_guid] = guid_list
            # print(f"[DEBUG] Canonical {canonical_id}: rappresentativo={representative_guid}, gruppo={guid_list}") # Only for very detailed debug
        else:
            print(f"[DEBUG WARN] Group canonical_id {canonical_id} has an empty guid list.")

    return representative_mapping

def get_latest_processed_timestamp(spark_session):
    """
    Recovers the maximum timestamp already processed from the curated layer.
    """
    try:
        if DeltaTable.isDeltaTable(spark_session, CURATED_PATH):
            df_curated = spark_session.read.format("delta").load(CURATED_PATH)
            max_ts = df_curated.select(spark_max("timestamp")).collect()[0][0]
            print(f"[DEBUG] Timestamp max founded in curated layer: {max_ts}")
            return max_ts
        else:
            print("[DEBUG] Table Delta in CURATED_PATH not exists yet.")
            return None
    except Exception as e:
        # This catch is broad, consider refining if specific errors are known
        print(f"[DEBUG] Error retrieving max timestamp: {str(e)}. The table may be empty or not exist or not have the 'timestamp' column.")
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

print("[DEBUG] Job started: complete join with all comments + Qdrant filter")

while True:
    now = time.time()

    # Reload Europeana if necessary
    if europeana_df is None or (now - last_europeana_reload > RELOAD_EUROPEANA_MIN * 60):
        print("[DEBUG] Reloading Europeana metadata from Delta...")
        europeana_df = read_latest_delta_table(spark, EUROPEANA_PATH)
        europeana_df.cache() # Cache for multiple uses
        print(f"[DEBUG] Europeana DF rows: {europeana_df.count()}")
        last_europeana_reload = now

    # Reload validated IDs from Qdrant if necessary
    if canonical_groups is None or (now - last_qdrant_reload > RELOAD_QDRANT_MIN * 60):
        canonical_groups = get_all_validated_canonical_groups_from_qdrant()
        representative_mapping = get_representative_guids_from_canonical_groups(canonical_groups)
        last_qdrant_reload = now

    if not canonical_groups:
        print("[DEBUG] No canonical_id group validated by Qdrant. No data to process. Waiting...")
        time.sleep(60)
        continue

    # Finds the latest processed timestamp from the curated layer
    latest_ts = get_latest_processed_timestamp(spark)
    print(f"[DEBUG] Latest processed timestamp in curated: {latest_ts}")

    # Read all annotations from UGC
    ugc_df = spark.read.format("delta").load(UGC_PATH)
    ugc_df.cache() # Cache for multiple uses
    print(f"[DEBUG] UGC DF total rows: {ugc_df.count()}")

    # For incremental processing, filter only new annotations if necessary
    if latest_ts:
        ugc_df_incremental = ugc_df.filter(col("timestamp") > lit(latest_ts))
        print(f"[DEBUG] UGC total: {ugc_df.count()}, UGC new (timestamp > {latest_ts}): {ugc_df_incremental.count()}")

        if ugc_df_incremental.count() == 0:
            print("[DEBUG] No new UGC annotation found. Waiting...")
            ugc_df.unpersist() # Unpersist if not used further in this iteration
            europeana_df.unpersist() # Unpersist if not used further in this iteration
            time.sleep(60)
            continue
    else:
        ugc_df_incremental = ugc_df
        print(f"[DEBUG] First run: processing every UGC ({ugc_df.count()} rows)")

    # First do a complete join between Europeana and UGC, then filter for Qdrant
    print("[DEBUG] Performing LEFT JOIN between Europeana and UGC...")

    # Complete Join: all Europeana data + any UGC annotations
    complete_joined_df = europeana_df.join(ugc_df, on="guid", how="left") \
                                     .withColumn("joined_at", current_timestamp())
    complete_joined_df.cache() # Cache for subsequent operations
    current_count = complete_joined_df.count()
    print(f"[DEBUG] Complete join result Europeana + UGC: {current_count} rows")
    
    if current_count == 0:
        print("[DEBUG] Complete join result Europeana + UGC is empty. Waiting...")
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # Now apply the Qdrant filter: keep only the guids that are present in Qdrant
    # Create a list of all validated guids (all guids from all groups)
    all_validated_guids = []
    for guid_list in canonical_groups.values():
        all_validated_guids.extend(guid_list)
    
    all_validated_guids = list(set(all_validated_guids))  # remove duplicates
    print(f"[DEBUG] Total validated guids from Qdrant: {len(all_validated_guids)}")

    # Filter the complete join to include only the guids present in Qdrant
    qdrant_filtered_df = complete_joined_df.filter(col("guid").isin(all_validated_guids))
    qdrant_filtered_df.cache()
    current_count = qdrant_filtered_df.count()
    print(f"[DEBUG] Rows after Qdrant filter: {current_count}")

    if current_count == 0:
        print("[DEBUG] No rows after Qdrant filter. Waiting...")
        qdrant_filtered_df.unpersist()
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # Now apply the temporal filter for incremental processing
    if latest_ts:
        # Keep: 1) rows without annotations (timestamp is null)
        #           2) rows with new annotations (timestamp > latest_ts)
        final_df = qdrant_filtered_df.filter(
            col("timestamp").isNull() | (col("timestamp") > lit(latest_ts))
        )
    else:
        final_df = qdrant_filtered_df
    
    final_df.cache()
    current_count = final_df.count()
    print(f"[DEBUG] Rows after temporal filter: {current_count}")

    if current_count == 0:
        print("[DEBUG] No rows after final temporal filter. Waiting...")
        final_df.unpersist()
        qdrant_filtered_df.unpersist()
        complete_joined_df.unpersist()
        ugc_df.unpersist()
        europeana_df.unpersist()
        time.sleep(60)
        continue

    # aggregation for CANONICAL_ID:
    print("[DEBUG] Starting aggregation for canonical_id...")

    # Create a mapping from guid to canonical_id for grouping
    guid_to_canonical = {}
    for canonical_id, guid_list in canonical_groups.items():
        for guid in guid_list:
            guid_to_canonical[guid] = canonical_id

    # Add canonical_id column to DataFrame
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    def get_canonical_id_udf(guid): # Renamed to avoid conflict
        return guid_to_canonical.get(guid, guid) # If not found, use the same guid
    
    canonical_udf = udf(get_canonical_id_udf, StringType())
    
    final_df_with_canonical = final_df.withColumn("canonical_id", canonical_udf(col("guid")))
    final_df_with_canonical.cache()
    print(f"[DEBUG] rows after adding canonical_id: {final_df_with_canonical.count()}")

    # Separate rows with and without comments
    with_comments = final_df_with_canonical.filter(col("timestamp").isNotNull())
    without_comments = final_df_with_canonical.filter(col("timestamp").isNull())

    print(f"[DEBUG] Rows with comments (before canonical processing): {with_comments.count()}")
    print(f"[DEBUG] Rows without comments (before canonical processing): {without_comments.count()}")

    # For rows without comments, take only the representative guid for each canonical_id
    # Ensure representative_mapping is not empty before creating the list
    if representative_mapping:
        representative_guids_list = [rep_guid for rep_guid in representative_mapping.keys()]
        representative_without_comments = without_comments.filter(col("guid").isin(representative_guids_list))
    else:
        representative_without_comments = spark.createDataFrame([], without_comments.schema) # Empty DF with same schema
        print("[DEBUG WARN] representative_mapping is empty, no representative rows without comments will be created.")

    representative_without_comments.cache()
    print(f"[DEBUG] Representative rows without comments (after filter): {representative_without_comments.count()}")

    # For rows with comments, update the guid to the representative while keeping all comments
    if with_comments.count() > 0:
        # Mapping from guid to representative
        guid_to_representative = {}
        for rep_guid, guid_list in representative_mapping.items():
            for guid in guid_list:
                guid_to_representative[guid] = rep_guid
        
        def get_representative_guid_udf(guid): # Renamed to avoid conflict
            return guid_to_representative.get(guid, guid)
        
        representative_udf = udf(get_representative_guid_udf, StringType())

        # Update the guid to the representative for comments
        with_comments_representative = with_comments.withColumn(
            "guid", representative_udf(col("guid"))
        )
        
        # Join with metadata Europeana of rapresentative guid
        # before obtaining only the metadata Europeana for the representative guids
        if representative_mapping: # Ensure representative_mapping is not empty
            europeana_representative = europeana_df.filter(
                col("guid").isin([rep_guid for rep_guid in representative_mapping.keys()])
            )
        else:
             europeana_representative = spark.createDataFrame([], europeana_df.schema) # Empty DF with same schema
             print("[DEBUG WARN] representative_mapping is empty, no representative Europeana metadata will be joined.")

        # Join to obtain the correct metadata
        # Select columns to avoid ambiguity if 'guid' is in both
        europeana_columns = [c for c in europeana_df.columns if c != "guid"] # All Europeana cols except guid
        ugc_columns_for_join = [c for c in with_comments_representative.columns if c not in europeana_columns and c != "guid"]
        
        with_comments_final = with_comments_representative.select("guid", *ugc_columns_for_join).join(
            europeana_representative.select("guid", *europeana_columns), on="guid", how="left"
        ).withColumn("joined_at", current_timestamp())
        
        with_comments_final.cache()
        print(f"[DEBUG] rows with comments after join with representative Europeana: {with_comments_final.count()}")

        # combines rows with and without comments
        # Ensure schemas are aligned before union
        # Get common columns
        common_cols = list(set(representative_without_comments.columns) & set(with_comments_final.columns))
        final_result_union_aligned = representative_without_comments.select(common_cols).unionByName(with_comments_final.select(common_cols))
        final_result_union_aligned.cache()
        print(f"[DEBUG] Result of join of rows with and without comments: {final_result_union_aligned.count()}")

        final_result = final_result_union_aligned
    else:
        print("[DEBUG] no comments to process with representative logic.")
        final_result = representative_without_comments
    
    final_result.cache()
    print(f"[DEBUG] Final result before deduplication: {final_result.count()}")

    #  final deduplication
    final_result_deduplicated = final_result.dropDuplicates(["guid", "user_id", "timestamp"])
    final_result_deduplicated.cache() # Cache before final count and write
    
    row_count = final_result_deduplicated.count()
    print(f"[DEBUG] Final result after deduplication: {row_count}")

    print(f"[METRICHE] Object Europeana total: {europeana_df.count()}")
    print(f"[METRICHE] Object Europeana validated (are in Qdrant): {len(all_validated_guids)}")
    print(f"[METRICHE] Groups canonical_id: {len(canonical_groups)}")
    print(f"[METRICHE] Rows to write: {row_count}")
    
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
    print(f"[DEBUG] Elaboration completated. Rows to write with MERGE: {row_count}")

    if row_count > 0:
        # Verify if the Delta table exists, otherwise create it
        if not DeltaTable.isDeltaTable(spark, CURATED_PATH):
            print(f"[DEBUG] The Delta table at '{CURATED_PATH}' does not exist. Initial creation of the table.")
            final_result_deduplicated.write.format("delta").mode("overwrite").save(CURATED_PATH)
            print(f"[DEBUG] Delta table created with overwrite at '{CURATED_PATH}'.")
        else:
            deltaTable = DeltaTable.forPath(spark, CURATED_PATH)

            print("[DEBUG] Executing MERGE in the curated layer...")
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
            print("[DEBUG] Writing completed in the curated layer via MERGE.")
    else:
        print("[DEBUG] No rows to write via MERGE.")

    print("[DEBUG] Waiting 70 seconds before the next cycle...")
    time.sleep(70)