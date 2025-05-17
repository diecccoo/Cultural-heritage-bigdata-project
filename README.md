# Cultural-heritage-bigdata-project

This project aims to design and prototype a big data system that supports the digitization, analysis, and exploration of large-scale cultural heritage collections. These include artifacts, manuscripts, images, and user-contributed metadata from museums, libraries, and historical archives.



## spark-apps/

This folder is shared with the Spark container. It includes:

- `spark_stream.py`: consumes Kafka messages and writes Parquet to MinIO
- `europeana_ingest.py`: fetches images and metadata from Europeana and uploads them to MinIO
- `multi_ingest.py`: reads queries.txt and triggers ingestion for each query
- `queries.txt`: list of topics to ingest from Europeana
