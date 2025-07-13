# Configuration for Kafka, MinIO, and PostgreSQL

This folder contains initialization and configuration files for the services used in our data pipeline: **Kafka**, **MinIO**, and **PostgreSQL**.

These files are used inside Docker containers and support the correct startup and structure of the ingestion, storage, and query layers of the system.

---

## Kafka

### `kafka/create_kafka_topics.sh`
A shell script executed in a Kafka container to create the required topics at startup. It currently creates:

- `user_annotations` – for user-generated content
- `europeana_metadata` – for metadata ingested from the Europeana API

Each topic is created with **1 partition** and **1 replica**, using the `kafka-topics.sh` CLI tool.

### `kafka/server-1.properties`

Kafka broker configuration file mounted inside the Kafka container to customize its behavior. Key settings include:

- `listeners=PLAINTEXT://kafka:9092`  
  Defines the internal address where the broker listens for connections. This matches the Docker Compose service name and port.

- `advertised.listeners=PLAINTEXT://kafka:9092`  
  Ensures that Kafka advertises a reachable hostname (`kafka`) to other containers in the Docker network. Without this, Spark and other services might fail to connect.

- `auto.create.topics.enable=true`  
  Allows Kafka to automatically create topics when a producer sends a message to a non-existing topic. 

- `log.dirs=/var/lib/kafka/data`  
  Path where Kafka stores its internal message logs. This is mounted to a Docker volume for persistence.


#### Architectural Notes: Single Broker, Single Partition

In this prototype, Kafka topics are created with:

- **1 broker**
- **1 partition**
- **1 replica**

This setup is intentionally minimal to allow the system to run locally with limited resources. However, it comes with important limitations:

- **No parallelism**: with only one partition, messages are consumed sequentially. This limits throughput and prevents horizontal scaling.
- **No redundancy**: a single replica means no fault tolerance. If the broker fails, data is lost.
- **Single point of failure**: with one broker, the whole messaging system can go down.

We are aware of these limitations since we chose this configuration to ensure the system remains runnable on standard development machines, without requiring the overhead of running multiple Kafka brokers or high-availability setups.

#### How to scale in a real world big data system

In a production scenario, you would typically:
- Use **at least** 2 brokers to distribute load and improve fault tolerance
- Define **multiple partitions** per topic, especially for high-throughput streams like `user_annotations`
- Add **replicas** to ensure message durability and broker failover

For example, adding partitions to the `user_annotations` topic could allow:
- Parallel ingestion from multiple annotation sources
- Faster and scalable Spark consumption
- Better support for real-time user interaction scenarios in museums or exhibitions

---

## MinIO

### `minio/init_minio.py`
Python script used to initialize the `heritage` bucket in MinIO and create the internal folder structure used by the pipeline. Specifically, it:

- Connects to MinIO using `boto3` with credentials user: `minio` and  password: `minio123`
- Waits for MinIO to become reachable
- Creates the following folders in the bucket:
    ```
    raw/metadata/europeana_metadata/
    raw/metadata/user_generated_content/
    cleansed/europeana/
    cleansed/user_generated/
    curated/join_metadata/
    ```

    These directories reflect the structure of our data lake:
    - `raw/` holds the original JSON data ingested from Kafka
    - `cleansed/` stores deduplicated and cleaned Delta tables
    - `curated/` contains joined and enriched datasets ready for querying or serving

#### Delta Lake Alignment

This folder layout reflects the principles of a **Delta Lake** architecture, which separates data into three logical layers:

- `raw/` – append-only ingested data in JSON format, in this case from Kafka
- `cleansed/` – deduplicated and structured Delta tables
- `curated/` – finalized, enriched datasets ready to be served to applications or dashboards

This layered approach improves traceability and data governance, while allowing each stage to evolve independently.

By enforcing this structure at bucket initialization, we ensure that:
- Every Spark consumer or processing job writes to the correct location
- Data lineage is preserved from ingestion to serving
- Future extensions (e.g., audit logs, versioning, time travel) can be added without reworking the layout

### `minio/Dockerfile`
Custom Dockerfile used to build the container that runs `init_minio.py`. This ensures that bucket initialization logic is isolated and runs only once on startup.

---

## PostgreSQL --> importante aggiornare con il collegamento per streamlit

### `postgres/init.sql`
SQL script automatically executed when the PostgreSQL container starts for the first time. It currently creates the following table:

- `join_metadata_deduplicated` – used in the **serving layer** to store cleaned and joined Europeana metadata and user annotations.

---

## File Structure

```text
config/
├── kafka/
│   ├── create_kafka_topics.sh       # Creates required Kafka topics on startup
│   └── server-1.properties          # Custom configuration for the Kafka broker
│
├── minio/
│   ├── init_minio.py                # Creates 'heritage' bucket and internal folders
│   └── Dockerfile                   # Container used to run MinIO initialization
│
├── postgres/
│   └── init.sql                     # Initializes join_metadata table in PostgreSQL
```
---

The files in this folder define the initial state and structure of the system's core services. While they are executed inside Docker containers, this folder keeps them versioned, editable, and ready for extension if needed.