# ML Models

This folder contains machine learning components of our pipeline. These modules run in Docker containers and are responsible for:

1. **Extracting multimodal embeddings** (image + text) from cleansed metadata
2. **Deduplicating similar items** in the vector store (Qdrant) based on image similarity

They operate downstream of the Spark-based cleansing pipeline and upstream of any clustering or recommendation logic.
Qdrant is used as a **vector database**, allowing us to store and search high-dimensional embeddings for semantic deduplication and similarity-based retrieval.
---

## 📌 Architecture Overview
## 🔄 Workflow Summary

This ML module runs after the Spark cleansing stage and operates as follows:

```text
cleansed/europeana/ (Delta table on MinIO)
        │
        ▼
[embeddings-extractor]
  → Loads new records from the cleansed layer
  → Downloads image + builds text prompt
  → Computes embeddings with CLIP
  → Uploads embeddings to Qdrant with status = "pending"
        │
        ▼
Qdrant vector DB
        │
        ▼
[qdrant-deduplicator]
  → Scans for pending points
  → Finds similar validated embeddings
  → Assigns a shared canonical_id to duplicates
  → Updates point status to "validated"
        │
        ▼
Canonicalized archive inside Qdrant
  → All vectors have consistent canonical_id
  → Ready for clustering and recommendation
```

This design allows asynchronous processing and semantic deduplication of cultural objects based on image similarity.

OPPURE

## 🧭 Workflow Diagram (Mermaid)

```mermaid
flowchart TD
    A[Delta Table: cleansed/europeana/] --> B[embeddings-extractor]
    B --> B1[Download image & build prompt]
    B1 --> B2[Compute embeddings (CLIP)]
    B2 --> B3[Push to Qdrant with status="pending"]

    B3 --> C[Qdrant DB]
    C --> D[qdrant-deduplicator]
    D --> D1[Search for similar validated embeddings]
    D1 --> D2[Assign canonical_id if duplicate]
    D2 --> D3[Update status="validated"]

    D3 --> E[Canonicalized Qdrant collection]
    E --> F[→ ready for clustering, search, recommendations]
```
This diagram summarizes the two-stage ML pipeline: embedding generation and semantic deduplication via Qdrant.

---

## 1. `embeddings-extractor/`

This module runs the `extracting_embeddings.py` script in a loop. It processes cleaned Europeana records and generates embeddings using the HuggingFace `CLIP` model.

We chose CLIP because it embeds both images and natural language into a shared semantic space. This is particularly suitable for our task, where we want to:

- Represent cultural objects using both **visual** (e.g., paintings, artifacts) and **textual** (e.g., title, description, tags) information
- Support **cross-modal** similarity for clustering and recommendations
- Use a single unified model without task-specific fine-tuning

### What it does

The script: 
- Reads new records from `s3a://heritage/cleansed/europeana/` (Delta Lake format)
- Downloads images from the `isShownBy` field (image URL)
- Builds a text prompt from fields: `title`, `subject`, `creator`, `type`, `description`
- Extracts:
  - `embedding_image` (512-dim vector)
  - `embedding_text` (512-dim vector)
  - `combined` (1024-dim vector) used for recommendations
- Uploads the result to `Qdrant` with:
  - `status = pending`
  - `id_object = guid`
  - `embedding_status = OK / FAILED / NO_IMAGE`
- Stores progress using `embedding_last_processed.txt` in MinIO, to keep track of the last processed image

### Output format 

```json
{
  "id_object": "2021401/https___data.europeana.eu_item_12345",
  "status": "pending",
  "embedding_image": [...],
  "embedding_text": [...],
  "embedding_status": "OK"
}
```

### Design choices

- **Batching**: Spark loads 112 records per cycle; embedding batches are limited to 16 for GPU/CPU safety.
- **Resilience**: If the image cannot be downloaded, the script still extracts the text embedding and sets the `embedding_status` to `NO_IMAGE`. If any embedding step fails, it sets the status to `FAILED`.
- **Stateless container**: The last processed GUID is stored in MinIO as a plain text file, allowing the loop to resume from the correct point.
- **Loop execution**: The process runs continuously with a `sleep(60)` delay between iterations (sleep can be configured).

> **Note**:  
> The first time the `embeddings-extractor` container is built, it may take several minutes (up to 10–15 min) due to:
> - The installation of heavy libraries (`torch`, `transformers`, etc.)
> - The download of the CLIP model from Hugging Face
> 
> To reduce startup time in future runs, a persistent Hugging Face cache is mounted in the `docker-compose.yml` via:
> 
> ```yaml
> volumes:
>   - ./hf-cache:/root/.cache/huggingface/transformers
> ```

---

## 2. `qdrant-deduplicator/`

This container runs the `deduplicate_from_qdrant.py` script, which scans Qdrant for points with `status = pending`, searches for semantic duplicates, and assigns a canonical identifier.

### What It Does

- Queries Qdrant for points where `status = pending`
- For each pending point:
  - Uses the `embedding_image` vector to search for similar points already marked as `validated`
  - If the top match has cosine similarity ≥ `0.98`, the same `canonical_id` is reused
  - If no match is similar enough, a new `canonical_id` is generated using `uuid4`
- The point's payload is updated in Qdrant with:
  - `status: validated`
  - `canonical_id: ...`
  - `processed_at: ...` (timestamp of deduplication)

### Fallback mechanism

If the Qdrant `search()` API fails or returns no candidates:
- The script uses `scroll()` to retrieve all previously validated points
- Cosine similarity is computed manually using `scikit-learn`'s `cosine_similarity` function


### Final output (Qdrant Point Payload)
```json
{
  "status": "validated",
  "canonical_id": "1f9ef...d2a1",
  "processed_at": "2025-07-13T10:42:00"
}
```

### Design notes

- Uses **batched scroll** to avoid memory overload during fallback retrieval
- Skips points with missing `embedding_image` values
- Logs progress and statistics at each iteration

---

## File structure

```text
ML-model/
├── embeddings-extractor/
│   ├── extracting_embeddings.py     # Main embedding loop (CLIP + Spark + Qdrant)
│   ├── requirements.txt             # Dependencies: PySpark, Transformers, Qdrant, MinIO
│   ├── Dockerfile                   # Embedding container definition
│
├── qdrant-deduplicator/
│   ├── deduplicate_from_qdrant.py   # Deduplication logic with fallback strategy
│   ├── Dockerfile                   # Deduplication container definition
│
└── README.md                        # This document
```

---

## Notes & limitations

- This pipeline assumes all image URLs are reachable and valid; if image download or embedding fails, the error is logged but **not retried**
- Deduplication is performed **only on image embeddings** to reduce the risk of false positives from textual similarity SIAMO SICURI?
- No points are deleted from Qdrant: all embeddings remain available for future clustering, recommendation, or analytics tasks

---

##  Future improvements

- Implement retry logic for transient failures in image downloading or embedding computation
- Allow points with `status = "failed"` to be retried after a timeout or under manual intervention
- Explore a hybrid deduplication strategy that combines **image** and **text** embeddings to improve accuracy
