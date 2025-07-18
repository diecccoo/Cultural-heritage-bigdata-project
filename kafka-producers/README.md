# Kafka producers for simulated ingestion

This directory contains two Kafka producers responsible for simulating data ingestion in a scalable big data system designed for museums, libraries, and heritage sites.

The system is structured around a modular data pipeline, and these producers represent the **first stage**, injecting data into Kafka topics. From there, Spark consumers process the streams and write to the RAW layer in MinIO.

## Project Context

Our system is designed with the scenario of a **museum** or **heritage institution** self-uploading its metadata and images. Since real ingestion from cultural partners is not feasible in this prototype, we simulate it in two ways:

- The **Europeana producer** fetches metadata from the [Europeana API](https://pro.europeana.eu/page/apis), mimicking ingestion from external cultural providers (See "How to Run" section on the main README).
- The **Annotation producer** simulates user-generated content (for example tags and comments) on existing objects.

---

## Producers Overview

### `europeana_ingest_batch.py` – Europeana Metadata Producer

This script fetches metadata from the Europeana API using a fixed list of provider IDs. For each pre-defined provider, it retrieves a number of pages (configurable via `PAGES_PER_HOUR`) with 100 records per page (configurable via `ROWS_PER_PAGE`). The use of `cursor` allows scrolling within a single session.

#### Key Features

- Simulates a museum uploading its own metadata 
- Publishes records to Kafka topic `europeana_metadata`
- Avoids duplicates via local file `downloaded_guids.txt`
- Skips image download to avoid overloading the pipeline, but saves its url in the metadata
- Uses `scheduler.py` to periodically execute the script

#### Known Limitations

Due to the design of the Europeana API:
- The `cursor` mechanism **only works in continuous sessions**; we cannot resume precisely where we left off between different API calls.
- `start` values above 1000 are **not supported** by the Europeana API, which limits pagination without cursor.
- We cannot guarantee full coverage of a provider unless `PAGES_PER_HOUR` is sufficiently large.

These constraints are acceptable in our architecture, as the script is intended to **simulate ingestion**, not serve as a complete harvesting solution.

#### Sample Metadata Message

```json
{
  "title": "Portrait of a Woman",
  "guid": "2021401/https___data.europeana.eu_item_12345",
  "image_url": "https://europeanaphotos.org/img/123.jpg",
  "timestamp_created": "2025-07-09T08:43:12",
  "provider": "europeana_provider_id",
  "description": "A beautiful portrait from the Renaissance era.",
  "creator": "Anonymous Italian painter",
  "subject": ["portrait", "woman", "renaissance"],
  "language": "en",
  "type": "IMAGE",
  "format": "image/jpeg",
  "rights": "© Europeana",
  "dataProvider": "Museo Nazionale Virtuale",
  "isShownBy": "https://europeanaphotos.org/img/123.jpg",
  "edm_rights": "http://rightsstatements.org/vocab/InC/1.0/"
}
```
### `annotation_producer.py` – User Annotation Producer

This script generates user annotations and comments on top of existing objects. It retrieves object IDs from the Delta table in MinIO (`heritage/cleansed/europeana`) and creates synthetic tags, comments, timestamps, and optional locations.

The messages are written to the Kafka topic `user_annotations`.

#### Key Features

- Produces realistic tags and natural language comments
- Simulates one annotation every few seconds
- Runs in a continuous loop using `time.sleep()`

#### Sample Annotation Message

```json
{
  "guid": "2021401/https___data.europeana.eu_item_12345",
  "user_id": "user_42",
  "tags": ["portrait", "woman", "renaissance"],
  "comment": "This painting reminds me of early Italian masters.",
  "timestamp": "2025-07-10T10:32:45",
  "location": "Verona"
}
```

## How Ingestion Works

1. Both producers publish messages to Kafka topics:
   - `europeana_metadata`
   - `user_annotations`

2. These messages are not written directly to **MinIO**.

3. Spark Structured Streaming consumers read from Kafka and write:
   - Raw Europeana metadata to `s3a://heritage/raw/metadata/europeana_metadata/`
   - Raw annotations to `s3a://heritage/raw/metadata/user_generated_content/`

In this way, we ensure:
- Decoupled, scalable ingestion
- Fault-tolerant persistence
- Independent evolution of producer and consumer logic

---

## Containerization

While both producers can be run manually via Python, they are designed to be used within a **Docker Compose** setup. In particular:

- `europeana_ingest_batch.py` is executed periodically by `scheduler.py`, which runs in a long-running container.
- `annotation_producer.py` can run indefinitely in its own container.

The system assumes the presence of:
- A running Kafka cluster (with topics pre-created)
- Spark + MinIO services for downstream processing

---

## Notes & Future Improvements

- Europeana scraping is purposefully lightweight and avoids downloading images. All image references are stored as URLs (`image_url`).
- The current architecture assumes append-only, immutable ingestion in the RAW layer.
- In the future, we could:
  - Add other sources of images and metadata other then Europeana
  - Use annotations sources such as Wikipedia media archives and real user-generated content from social platforms
  - Add image download as a separate batch process, to keep track of everything in the raw layer

---

## File Structure

```
kafka-producers/
├── annotation-producer/
│   ├── annotation_producer.py       # Simulates user-generated annotations on cultural objects
│   └── Dockerfile                   # Container for running the annotation producer
│
├── europeana-ingestion/
│   ├── europeana_ingest_batch.py    # Fetches metadata from Europeana API and sends to Kafka
│   ├── Dockerfile                   # Container for the Europeana producer and scheduler
│   ├── scheduler.py                 # Periodically runs the Europeana ingestion script
│   ├── requirements.txt             # Python dependencies for europeana-ingestion
│   ├── .env                         # Contains API key for Europeana 
│   └── state/
│       ├── downloaded_guids.txt    # Tracks which GUIDs have already been ingested
│       └── europeana_offset.json   # Stores current scroll state per provider
│
└── README.md                        # This documentation file 

```

- The `.env` file stores sensitive configuration (such as your Europeana API key) and is **excluded from version control** (`.gitignore`).
- Make sure to create your own `.env` in `europeana-ingestion/` with: API_KEY="your_europeana_api_key"