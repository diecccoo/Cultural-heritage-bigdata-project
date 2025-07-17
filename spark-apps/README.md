# Spark Applications 

This folder contains all **Spark-based** applications of our pipeline.  
These scripts are responsible for ingesting, cleansing, transforming, and joining data flowing from Kafka to MinIO (Delta Lake), and finally to PostgreSQL.

They are logically grouped based on the Delta Lake architecture:

- `raw` → ingestion from Kafka (landing in MinIO as raw JSON)
- `cleansed` → deduplicated, parsed Delta tables
- `curated` → enriched and joined objects ready for serving

Each subfolder contains a standalone Spark application that runs either in **streaming** (Spark structured streaming) or **batch mode**.

---

## Raw Ingestion from Kafka

### `eu-to-raw/metadata_eu_to_raw.py`

Spark Structured Streaming consumer that:
- Connects to the Kafka topic `europeana_metadata`
- Parses each message using a defined Europeana metadata schema
- Writes each record as a separate JSON file into `s3a://heritage/raw/metadata/europeana_metadata/`
- Uses `foreachBatch()` for fine-grained control over file naming (GUID-based)

This script simulates the ingestion of cultural metadata into the raw layer, following the Europeana API structure. Checkpoints are stored to ensure fault tolerance.

---

### `ugc-to-raw/kafka_annotations_to_minio_raw.py`

Kafka consumer (with Spark Structured Streaming) that:
- Reads from topic `user_annotations`
- Adds `ingestion_time`, `source`, and `dt` (partitioning key)
- Coalesces each batch into a single file
- Writes JSON to `s3a://heritage/raw/metadata/user_generated_content/dt=YYYY-MM-DD/`
- Uses micro-batches triggered every **30 seconds**

This script simulates user annotations being ingested at high frequency, mimicking real-time input.

---

## Cleansing and Transformation (to Delta)

### `ugc-to-cleansed/ugc_raw_to_cleansed.py`

Batch Spark job for cleansing user-generated content. It:
- Reads raw JSON files from `s3a://heritage/raw/metadata/user_generated_content/`
- Parses each annotation and removes duplicates based on the tuple `(guid, user_id, comment, timestamp)` for maximum reliability
- Writes cleaned annotations to Delta Lake `s3a://heritage/cleansed/user_generated/`

Designed to be scheduled periodically via `scheduler.py`, present in the same subfolder.

---

### `eu-to-cleansed

Batch job that:
- Loads raw Europeana JSONs from `s3a://heritage/raw/metadata/europeana_metadata/`
- Filters out records with null or empty `guid` or missing image fields (`image_url`)
- Removes duplicate metadata entries based on `guid` to ensure uniqueness
- Writes valid records to `s3a://heritage/cleansed/europeana/`

Designed to be scheduled periodically via `scheduler.py`, present in the same subfolder.

This folder includes **two versions** of the script used to write to the cleansed Delta table:
>
> - `eu_raw_to_cleansed_merge.py` – currently in use.  
>   Uses a `MERGE` strategy to update the table incrementally, inserting only new records based on the `guid`.  
> - `eu_raw_to_cleansed_overwrite.py` – older version (still available).  
>   Overwrites the entire Delta table at each run, which is more computationally expensive and less efficient at scale.
>
> You can switch strategies by modifying the `Dockerfile` to run the desired script.

---

## Join stage: curated layer 

### `join-eu-ugc-qdrant-to-curated/join_eu_ugc_qdrant_merge.py`

This script continuously performs a **join** between Europeana metadata and user annotations, using only validated (non-duplicate) objects. 
The result is a clean, enriched view for each cultural heritage item, ready for analysis, export, and recommendation.

It merges three sources:
- **Europeana metadata** from `cleansed/europeana/`
- **User annotations** from `cleansed/user_generated/`
- **Deduplication check** from Qdrant (via `canonical_id`)

The common key used for the join is `guid`.

#### The process involves:

1. **Full outer join** between Europeana metadata and user annotations on `guid`.  
   This ensures that:
   - Europeana records without annotations are retained
   - Annotated items are enriched with metadata
   - The underlying assumption is that UGC' guids always come from europeana 

2. **Filtering by Qdrant deduplication**:  
   Only records whose `guid` appears in Qdrant as a canonical object (or as a 'duplicate mapped to a canonical_id') are preserved.
   Thanks to `canonical_id`, annotations originally attached to duplicate objects are correctly reassigned to the canonical version — so no user input is lost.

3. **Delta Table writing with merge logic**:  
   - The table is updated incrementally using `MERGE`:
     - If a match is found on `(guid, timestamp, user_id)`, the record is updated
     - If not, the annotation is inserted
   - This avoids duplicates and ensures idempotency.

   Previously, we used an `overwrite` strategy, but it proved computationally expensive.  
   The old implementation (`join_eu_ugc_qdrant_overwrite.py`) is still available and can be re-enabled by modifying the Dockerfile.

The result is a **Delta Table**, where each row represents one user annotation joined with the corresponding Europeana metadata, stored at: `s3a://heritage/curated/join_metadata_deduplicated/`

---

## Export to Serving Layer

### `curated-to-postgres/curated_to_postgres.py`

Final Spark job that:
- Reads the curated, deduplicated join table from `s3a://heritage/curated/join_metadata_deduplicated/` on MinIO
- Selects and maps all key fields (e.g., `guid`, `user_id`, `tags`, `comment`, `description`, `creator`, `image_url`, etc.)
- Writes data to PostgreSQL (`heritage` database), first to a staging table (`join_metadata_staging`), then refreshes the production table (`join_metadata_deduplicated`) via SQL
- Ensures full alignment with the dashboard and serving layer requirements

It prepares data for downstream usage and dashboard visualization.

---

## File Structure


```
spark-apps/
│
├── eu-to-raw/                 
│   ├── metadata_eu_to_raw.py                       # Kafka consumer: ingests Europeana metadata and writes to RAW (MinIO JSON)
│   └── Dockerfile                                  # Container definition for the EU → RAW Spark job
│
├── ugc-to-raw/                                   
│   ├── kafka_annotations_to_minio_raw.py           # Kafka consumer: ingests user annotations and writes to RAW (MinIO JSON)  
│   └── Dockerfile                                  # Container definition for the UGC → RAW Spark job
│
├── eu-to-cleansed/
│   ├── eu_raw_to_cleansed_merge.py                 # Batch job: parses and deduplicates Europeana raw JSON into Delta
│   ├── eu_raw_to_cleansed_overwrite.py             # Older version with overwrite
│   ├── scheduler.py                                # Periodic trigger for metadata cleansing job
│   └── Dockerfile                                  # Container for the EU → CLEANSING job and scheduler
│
├── ugc-to-cleansed/                                
│   ├── ugc_raw_to_cleansed.py                      # Batch job: cleans and deduplicates user annotations from raw JSON to Delta
│   ├── scheduler.py                                # Periodic trigger for annotation cleansing job
│   └── Dockerfile                                  # Container for the UGC → CLEANSING job and scheduler
│
├── join-eu-ugc-qdrant-to-curated/
│   ├── join_eu_ugc_qdrant_merge.py                 # Streaming job: joins UGC and metadata into the curated layer
│   ├── join_eu_ugc_qdrant_overwrite.py             # Version with overwrite
│   ├── requirements.txt                            # Python and Spark dependencies for the join jobs
│   └── Dockerfile                                  # Container for the join job
│
├── curated-to-postgres/
│   ├── curated_to_postgres.py                      # Batch job: exports curated data from MinIO to PostgreSQL table
│   └── Dockerfile                                  # Container for exporting curated layer to PostgreSQL
│
└── README.md

```
  