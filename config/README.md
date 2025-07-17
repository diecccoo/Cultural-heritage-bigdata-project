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

When using the create_kafka_topics.sh script, please ensure that the file uses Unix-style line endings (LF).
This is especially important if you're working on Windows, where text editors may default to Windows-style line endings (CRLF), which can cause unexpected behavior such as syntax errors (^M visible in shell).

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
    curated/join_metadata_deduplicated/
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

## PostgreSQL 

### `postgres/init.sql`
SQL script automatically executed when the PostgreSQL container starts for the first time. 
The `heritage` database includes two key tables used for storing curated data after the join between Europeana metadata and user-generated annotations deduplicated:

#### `join_metadata_deduplicated`
This is the **main serving table** containing cleaned, deduplicated, and joined data. Each row represents a single user annotation and the associated cultural metadata.

#### `join_metadata_staging`
A **temporary staging table**, used during batch insertions and merge operations before data is validated and moved to the main table.

Both tables share the same schema:

| Column Name   | Type      | Description                                              |
|---------------|-----------|----------------------------------------------------------|
| `id`          | SERIAL    | Auto-incremented primary key                            |
| `guid`        | TEXT      | Original Europeana object identifier                    |
| `user_id`     | TEXT      | Identifier of the user who submitted the annotation     |
| `tags`        | TEXT[]    | List of tags assigned by the user                       |
| `comment`     | TEXT      | User's textual comment                                  |
| `timestamp`   | TIMESTAMP | Timestamp of the annotation                             |
| `source`      | TEXT      | Source of ingestion (e.g., `kafka:user_annotations`)    |
| `creator`     | TEXT      | Author of the object                                    |
| `description` | TEXT      | Metadata field from Europeana                           |
| `edm_rights`  | TEXT      | Copyright/licensing information                         |
| `format`      | TEXT      | Format of the cultural object                           |
| `image_url`   | TEXT[]    | Array of image URL associated with the object           |
| `isshownby`   | TEXT[]    | Additional image or media links                         |
| `language`    | TEXT      | Language of the object                                  |
| `provider`    | TEXT      | Provider or institution source                          |
| `subject`     | TEXT[]    | Array of subjects / keywords                            |
| `title`       | TEXT      | Title of the cultural object                            |
| `type`        | TEXT      | Type/category (e.g., image, text, video)                |


### Accessing PostgreSQL via pgAdmin 

### Prerequisites 
- Docker and Docker Compose installed and running
- Services `postgres` and `pgadmin` must be active (`docker compose up -d`)

---

### 1.  Open pgAdmin in your browser 
Go to: [http://localhost:5050](http://localhost:5050)

Login credentials (defined in `docker-compose.yml`):
- **Email**: `admin@heritage.com`
- **Password**: `admin`

---

### 2.  Add the PostgreSQL server 
- Click on `Add New Server`  


**Tab: General**
- **Name**: `heritage` 

**Tab: Connection**
- **Host name/address**: `postgres`  
- **Port**: `5432`  
- **Maintenance database**: `heritage`  
- **Username**: `postgres`  
- **Password**: `postgres`  

Click `Save` to connect.

---

### 3.  Browse the database tables 
- Expand the server (`heritage`)
- Navigate to: `Databases → heritage → Schemas → public → Tables`
- Right-click on a table and select `View/Edit Data → All Rows` to see its content

If the server doesn't appear, right-click on `Servers` and choose `Refresh`.


**Integration with Streamlit**

The Streamlit dashboard connects to this table (join_metadata_deduplicated) in the heritage database to power all visualizations, filters (currently by creator, provider, tags), and search features.

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
│   └── init.sql                     # Initializes tables in PostgreSQL
│
└── README.md
---

The files in this folder define the initial state and structure of the system's core services. While they are executed inside Docker containers, this folder keeps them versioned, editable, and ready for extension if needed.  