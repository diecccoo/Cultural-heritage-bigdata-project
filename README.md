# Cultural Heritage Big Data Pipeline

## 1. Overview
> Breve introduzione al progetto: obiettivo, ambito applicativo, tecnologie principali.

This project aims to design and prototype a big data system that supports the digitization, analysis, and exploration of large-scale cultural heritage collections. These include artifacts, manuscripts, images, and user-contributed metadata from museums, libraries, and historical archives.

This project implements a full data pipeline for collecting, ingesting, processing, and serving cultural heritage data. It includes simulated user interactions and metadata ingestion into Kafka, followed by structured processing with Apache Spark and storage in MinIO using a Delta Lake architecture.
The pipeline supports semantic deduplication, metadata enrichment, and content-based recommendations. Deduplicated and enriched data is then exported from MinIO to PostgreSQL, which integrates with the serving layer to power structured queries and dashboard visualizations.

The Streamlit dashboard lets users explore cultural content through search, filtering, and similarity-based navigation.

---

## 2. System architecture

### 2.1 Architecture overview

![Architecture](readme_images/system_architecture.jpeg)

### 2.2 Data flow diagram

![Data Flow](readme_images/data_flow_diagram.png)

### 2.3 File structure

---

## 3. Data Sources

### 3.1 Europeana Metadata

Europeana metadata is collected using the `europeana_ingest_batch.py` script, which performs provider-based, paginated API queries, filters for objects with images, ensures uniqueness using GUIDs, and streams the cleaned metadata into the `europeana_metadata` Kafka topic.

**How data is collected:**
- The script queries the Europeana REST API, scrolling results for each provider with parameters such as type (IMAGE), language (en), and cursor-based pagination.
- Only records with a valid image URL (`edmIsShownBy` or `isShownBy`) and a unique `guid` are ingested.
- The process avoids duplicates by keeping track of downloaded GUIDs in the file `downloaded_guids.txt`.
- The collected metadata is sent as JSON messages to Kafka for downstream processing.

**Key fields extracted for each object:**
- `guid`: Unique Europeana identifier (primary key for all downstream tables)
- `title`: Object title
- `image_url`: Direct URL to the object's image (`edmIsShownBy` or `isShownBy`)
- `timestamp_created`: Ingestion timestamp (UTC, ISO format)
- `provider`: Name of the Europeana data provider
- `description`: Description or abstract
- `creator`: Author or artist name
- `subject`: Main subject or tag
- `language`: Language code
- `type`: Type of object (e.g. IMAGE)
- `format`: Original format if available
- `rights`: Rights statement
- `dataProvider`: Source institution
- `isShownAt`: Link to the object page on the provider's website
- `edm_rights`: Rights in EDM schema

**Example JSON structure:**

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


--- 
### 3.2 User Annotations (Synthetic)

Synthetic user annotations are generated automatically using the `annotation_producer.py` script. The purpose is to simulate user engagement such as tagging, commenting, and localization on cultural heritage objects.

**How annotations are generated:**
- The script loads the list of all available object GUIDs from the cleansed Europeana metadata on MinIO.
- At regular intervals, it generates random annotations linked to these GUIDs.
- Each annotation simulates a real user, randomly selecting user IDs, tags, comments, and locations from pre-defined pools.

**Message structure:**
Each user annotation is sent to the Kafka topic `user_annotations` as a JSON object with the following fields:
- `guid`: Europeana object identifier (used as foreign key)
- `user_id`: Synthetic username or ID
- `tags`: List of 2–4 descriptive tags
- `comment`: Simulated free-text comment in English 
- `timestamp`: Annotation creation time (UTC, ISO format)
- `location`: User’s city or region 

**Sample Annotation Message**

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
---

## 4. Technologies Used

| Technology      | Role                                                                                                                     | Justification                                                                              |
|-----------------|--------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| **Kafka**           | Ingests Europeana metadata and user annotations; buffers, partitions, and distributes data streams to processing components. | Enables scalable and asynchronous ingestion of large volumes of data from heterogeneous sources. |
| **MinIO**           | Stores as a data lake Lake all ingested data (JSON, metadata, ugc) in a 3-layer Delta Lake Architecture (raw, cleansed, curated); provides S3 interface for Spark and other tools | Provides distributed storage and efficient access to data via S3-compatible APIs.          |
| **Spark**           | Cleanses, deduplicates, and transforms ingested data in both batch and streaming; maintains reliable ACID tables.         | Ensures parallel and reliable processing of large-scale data; unifies batch and streaming. |
| **CLIP (OpenAI)**   | Generates image embeddings (for deduplication and recommendations) and text embeddings (for recommendations).             | Provides semantic enrichment of objects and enables both deduplication and retrieval.      |
| **Qdrant**          | Indexes embeddings from CLIP; used for image deduplication and powering semantic search and recommendations.              | Enables fast vector search and similar object recommendations on the dashboard.            |
| **PostgreSQL**      | Hosts the final joined and deduplicated collection, supporting flexible queries and dashboard integration.                | Robust relational database for analytics and Streamlit integration.                        |
| **Streamlit**       | Provides an interactive web dashboard for searching, visualizing, and recommending deduplicated cultural heritage data.   | Enables exploration with data and recommendations.           |
| **Docker Compose**  | Orchestrates and isolates all services (Spark, MinIO, Qdrant, dashboard, etc.) for easy local or cloud deployment.        | Simplifies launching, managing, and reproducing the entire pipeline.                      |

---

## 5. Pipeline Stages
The pipeline is devided into specific layers: 

### 5.1 Ingestion Layer

In this layer, we ingest metadata from the [Europeana API](https://pro.europeana.eu/page/get-api), using a fixed list of cultural collections providers. Here, we simulate the scenario of a heritage site self-uploading its images and their metadata. In parallel, to simulate user interactions, we've built a producer of user-generated content that writes comments and tags on existing objects ingested from [Europeana](https://www.europeana.eu/it).

To do so, we have created two Kafka topics (`europeana_metadata` and `user_annotations`), where we publish our records, respectively ingested with a batch job from Europeana and produced every 0.5 seconds using a simulated event loop.

In the ingestion layer we don't write directly to MinIO, but we use Kafka to ensure decoupled, scalable ingestion and short-term fault-tolerant buffering before the data is processed and persisted to MinIO.

> It is really important to request your [API key](https://pro.europeana.eu/page/get-api), and create your `.env` file (ignored in the `.gitignore`) in the `europeana-ingestion/` folder to store your key as a variable.

### 5.2 Raw Storage Layer

In this layer, data is consumed from the Kafka topics and written to MinIO, which acts as our data lake. We adopt a **Delta Lake architecture**, organizing data into three layers: `raw/`, `cleansed/`, and `curated/`.

The raw layer stores append-only JSON records, preserving the original structure of the messages for traceability and reprocessing.

We chose Delta Lake on MinIO to ensure scalable, schema-aware storage with built-in versioning and ACID transactions. This architecture allows us to perform efficient batch and streaming transformations with Spark, while preserving data consistency, enabling schema enforcement, and supporting rollback via time travel. It also maintains a clear separation between raw input and processed outputs across the pipeline.

In this layer, data is consumed from Kafka and written to MinIO in append-only JSON format using Spark Structured Streaming. Europeana metadata is stored as one file per record, while user annotations are aggregated into a single file per batch and partitioned by date. 

### 5.3 Cleansing Layer
- Validazione
- Normalizzazione
- Output in Delta Lake

### 5.3 Cleansing Layer

In this layer, raw data is validated, cleaned, and transformed into Delta Tables stored on MinIO. We use Spark in batch mode to process both Europeana metadata and user-generated content. 

For user annotations, the cleansing job reads JSON files from the raw layer, validates the structure, and chek if there are duplicates. Cleaned records are written using Delta Lake in `append` mode. This strategy reflects the nature of annotations as high-frequency, additive content. 

For Europeana metadata, the job reads the JSON files, filters out malformed or incomplete records (e.g., null `guid` or missing `image_url`), removes duplicates on `guid`, and then writes the cleaned results in Delta format.

For this job, we initially used an `overwrite` strategy to fully replace the cleansed Europeana table at each run. Since this approach is computationally expensive and not optimized for frequent batch updates, we switched to a Delta `MERGE` strategy that incrementally inserts only new records based on `guid`, to improve the overall performance. In the future, the use of Delta Lake's `OPTIMIZE` command could enhance storage efficiency and query performance by compacting small files.

### 5.4 Machine Learning Model
- CLIP embedding (image + text)
- Salvataggio su Qdrant
- Deduplicazione semantica
- qdrant video
- whatsapp

### 5.5 Join to Curated Layer
- Join tra oggetti Europeana e annotazioni
- Uso di canonical_id
- Output finale (Delta + PostgreSQL)

### 5.6 Serving Layer
- PostgreSQL per dashboard e query
- Qdrant per recommendation
- Streamlit UI

---

## 6. How to Run
**PREREQUISITES**
Before starting the project, ensure the following requirements are met:

- **Docker and Docker Compose installed** on your machine  
  - [Get Docker](https://docs.docker.com/get-docker/)  
  - [Get Docker Compose](https://docs.docker.com/compose/install/)
- **`.env` file placed in the `kafka-producers/europeana-ingestion/` folder**  
  - This file should contain your Europeana API key and any custom environment variables required (e.g., `API_KEY=your_key_here`)

**RECOMMENDED**
- **At least 12 GB RAM available**
- **At least 10 CPU Cores** 

### 6.1 Docker Compose Setup

    1. Clone the repo: git clone https://github.com/dieccoo/Cultural-heritage-bigdata-project

    2. The `.env` file stores sensitive configuration (such as your Europeana API key) and is **excluded from version control** (`.gitignore`).
        - Make sure to create your own `.env` in `kafka-producers/europeana-ingestion/` with: API_KEY=your_europeana_api_key  

    3. Once inside the Cultural-heritage-bigdata-project directory run:   docker compose up --build –d  

### 6.1 Services, Volumes, and Network

- **Network**
  All containers are connected via a shared Docker network called **heritage-net**.  
  This network allows secure communication between services using service names.

- **Service dependencies**:  
  Several containers depend on others to start correctly (e.g., Kafka depends on Zookeeper, Spark jobs wait for MinIO and Kafka to be up, the dashboard waits for PostgreSQL and Qdrant). Docker Compose manages these dependencies to ensure correct startup order.

- **Volumes**:  
  Docker named volumes are used for data persistence. This means storage for MinIO, PostgreSQL, Qdrant, and shared intermediate data remains safe even if you restart or update containers. Without these volumes, your data would be lost at every restart.

- **Note on the dashboard**:  
  The Streamlit dashboard does **not automatically refresh** when you refresh the page.  
  **To see updated results, restart the dashboard container** with:
  ```sh
  docker compose restart streamlit

---

## 7. Example Usage

### 7.1 Dashboard Features
> Esplorazione, filtri, similar images, dettagli.

### 7.2 Query Examples
> Query su PostgreSQL (es. filtra per autore)  
> Query su Qdrant (es. simili per embedding)

---

## 10. Limitations & Future Work
> Cosa manca, cosa può essere migliorato, estensioni possibili.

---
## 11. References & Acknowledgments

- [PostgreSQL vs Redis](https://risingwave.com/blog/postgresql-vs-redis-performance-and-use-case-comparison/)

- [Spark vs Flink](https://www.datacamp.com/blog/flink-vs-spark)

- [Delta Lake documentation](https://delta.io/)

- [Delta Lake Architecture explenation](https://medium.com/codex/delta-lake-architecture-simplifying-data-engineering-analytics-needs-8d8be8459678)

- [Europeana API Key](https://pro.europeana.eu/page/get-api)


## 12. Authors & Contact
This project was developed by Group number 8 represented by:

1. Silvia Bortoluzzi - [@silviabortoluzzi](https://github.com/silviabortoluzzi)
2. Diego Conti - [@diecccoo](https://github.com/diecccoo)
3. Sara Lammouchi - [@saralammouchi](https://github.com/saralammouchi)

---
