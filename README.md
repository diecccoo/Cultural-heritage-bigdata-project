# Cultural-heritage-bigdata-project

This project aims to design and prototype a big data system that supports the digitization, analysis, and exploration of large-scale cultural heritage collections. These include artifacts, manuscripts, images, and user-contributed metadata from museums, libraries, and historical archives.

This project sets up a full data pipeline for collecting, ingesting, processing, and serving cultural heritage data. It supports user-generated content via MQTT, ingestion to Kafka, and streaming processing with Spark to MinIO.

## spark-apps/

This folder is shared with the Spark container. It includes:

- `spark_stream.py`: consumes Kafka messages and writes Parquet to MinIO
- `europeana_ingest.py`: fetches images and metadata from Europeana and uploads them to MinIO
- `multi_ingest.py`: reads queries.txt and triggers ingestion for each query
- `queries.txt`: list of topics to ingest from Europeana



##  How to Run the System

### 1. Start the infrastructure
Make sure you're in the root of the project and run:

```bash
docker-compose up --build
```

This starts:
- Mosquitto MQTT broker
- Kafka + Zookeeper
- Spark master and worker
- MinIO storage
- Redis, PostgreSQL
- MQTT-Kafka bridge container

---

### 2. Start Spark streaming job to write to MinIO

In another terminal, run the following inside the spark-master container:

```bash
docker exec -it spark-master spark-submit /opt/spark-apps/ingestion/kafka_to_raw.py
```

This will:
- Listen to `heritage_annotations` topic on Kafka
- Save all user-generated content as JSON files in `heritage/raw/metadata/` on MinIO

---

### 3. Simulate user-generated content

To test the ingestion pipeline, run:

```bash
python scripts/simulate_user_input.py
```

This will:
- Generate a fake user annotation (object ID, tags, comment)
- Publish it to the MQTT topic `heritage/annotations`
- Be bridged into Kafka and processed by Spark

---

### 4. Check output in MinIO

Go to [http://localhost:9001](http://localhost:9001)

- Username: `minioadmin`
- Password: `minioadmin`

You should find JSON files under:
```
heritage/raw/metadata/
```

---

## 📂 Directory Structure

Key folders:

- `spark-apps/ingestion/kafka_to_raw.py` — streaming job for ingestion
- `scripts/simulate_user_input.py` — simulates user crowdsourcing
- `mqtt-kafka-bridge/` — container to forward MQTT → Kafka
- `storage/minio/` — MinIO local storage

