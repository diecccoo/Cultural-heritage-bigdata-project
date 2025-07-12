# Spark Applications 

This folder contains all **Spark-based** applications of our pipeline.  
These scripts are responsible for ingesting, cleansing, transforming, and joining data flowing from Kafka to MinIO (Delta Lake), and finally to PostgreSQL.

They are logically grouped based on the Delta Lake architecture:

- `raw` → ingestion from Kafka
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

Batch job that:
- Reads raw JSON files from `s3a://heritage/raw/metadata/user_generated_content/`
- Removes duplicates based on the same attributes `(object_id, user_id, comment, timestamp)`
- Writes cleaned annotations to Delta Lake `s3a://heritage/cleansed/user_generated/`

Designed to be scheduled periodically via `scheduler.py`, present in the same subfolder.

---

### `eu-to-cleansed/metadata_to_delta_table.py`

Batch job that:
- Loads raw Europeana JSONs from `s3a://heritage/raw/metadata/europeana_metadata/`
- Filters out records with null `guid` or `isShownBy`
- Removes duplicate metadata entries
- Writes valid records to `s3a://heritage/cleansed/europeana/`

Designed to be scheduled periodically via `scheduler.py`, present in the same subfolder.

---

## 🔗 Join Stage: Curated Layer --> da aggiornare!

### `join-eu-ugc-curated/join_deltatables_eu_ugc.py`

A long-running Spark job that:
- Monitors the `user_generated/` Delta table
- Joins new annotations (by `object_id` → `guid`) with metadata from `europeana/`
- Appends the result to `s3a://heritage/curated/join_metadata/`

It keeps track of the most recent annotation timestamp and only processes new rows. Joins are done every 60 seconds, and Europeana metadata is refreshed every 15 minutes.

---

## Export to Serving Layer

### `curated-to-postgres/curated_to_postgres.py`

Final Spark job that:
- Reads the curated join table from `s3a://heritage/curated/join_metadata/`
- Selects and maps key fields: image, tags, comment, description, creator...
- Writes to PostgreSQL (`heritage` DB) in table `join_metadata` via JDBC

It prepares data for downstream usage and dashboard visualization.

---

## File Structure

```text
spark-apps/
├── eu-to-raw/
│   ├── metadata_eu_to_raw.py           # Kafka consumer: ingests Europeana metadata and writes to RAW (MinIO JSON)
│   └── Dockerfile                      # Container definition for the EU → RAW Spark job
│
├── ugc-to-raw/
│   ├── kafka_annotations_to_minio_raw.py   # Kafka consumer: ingests user annotations and writes to RAW (MinIO JSON)
│   └── Dockerfile                          # Container definition for the UGC → RAW Spark job
│
├── eu-to-cleansed/
│   ├── metadata_to_delta_table.py     # Batch job: parses and deduplicates Europeana raw JSON into Delta format
│   ├── scheduler.py                   # Periodic trigger for metadata cleansing job
│   └── Dockerfile                     # Container for the EU → CLEANSING job and scheduler
│
├── ugc-to-cleansed/
│   ├── ugc_raw_to_cleansed.py         # Batch job: cleans and deduplicates user annotations from raw JSON to Delta
│   ├── scheduler.py                   # Periodic trigger for annotation cleansing job
│   └── Dockerfile                     # Container for the UGC → CLEANSING job and scheduler
│
├── join-eu-ugc-curated/
│   ├── join_deltatables_eu_ugc.py     # Streaming job: joins UGC and metadata into the curated layer
│   └── Dockerfile                     # Container for the join job
│
├── curated-to-postgres/
│   ├── curated_to_postgres.py         # Batch job: exports curated data from MinIO to PostgreSQL table
│   └── Dockerfile                     # Container for exporting curated layer to PostgreSQL
│
└── README.md                          # This documentation file
```






