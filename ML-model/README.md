# ML Model

This folder contains machine learning components of our pipeline. These modules run in Docker containers and are responsible for:

1. **Extracting multimodal embeddings** (image + text) from cleansed metadata
2. **Deduplicating similar items** in the vector store (Qdrant) based on image similarity

They operate downstream of the Spark-based cleansing pipeline and upstream of any clustering or recommendation logic.
Qdrant is used as a **vector database**, allowing us to store and search high-dimensional embeddings for semantic deduplication and similarity-based retrieval.

---

## Architecture overview

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

This design allows asynchronous processing and semantic deduplication of our objects based on image similarity.

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
- Downloads images from the `image_url` field (falls back to `isShownBy` if needed)
- Builds a text prompt from fields: `title`, `subject`, `creator`, `type`, `description`
- Extracts:
  - `embedding_image` (512-dim vector)
  - `embedding_text` (512-dim vector)
- Concatenates text and image embeddings in a `embedding_combined` (1024-dim vector) used for recommendations
- Uploads the result to `Qdrant` with:
  - `status = pending`
  - `guid = the unique identifier for each object`
  - `embedding_status = OK / FAILED / NO_IMAGE`
- Stores progress using `embedding_last_processed.txt` in MinIO, to keep track of the last processed image

### Output format 

```jsonc
{
  "payload": {
    "guid": "2021401/https___data.europeana.eu_item_12345",
    "status": "pending"
  },
  "vectors": {
    "combined": [0.123, 0.456, 0.789  ],  // 1024-dimensional vector (image + text)
    "image": [0.101, 0.202, 0.303   ]      // 512-dimensional vector (image only)
  }
}
```

### Design choices

- **Batching**: Spark loads 64 records (LIMIT_RECORDS) per cycle; embedding batches are limited to 16 (BATCH_SIZE) for GPU/CPU safety (parameters are configurable).
- **Resilience**: If the image cannot be downloaded, the script still extracts the text embedding and sets the `embedding_status` to `NO_IMAGE`. If any embedding step fails, it sets the status to `FAILED`.
- **Stateless container**: The last processed GUID is stored in MinIO as a plain text file, allowing the loop to resume from the correct point.
- **Loop execution**: The process runs continuously with a `sleep(SLEEP_SECONDS)` delay between iterations (SLEEP_SECONDS can be configured).

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
  - If the top match has cosine similarity ≥ `0.97`(configurable via SIMILARITY_THRESHOLD), the same `canonical_id` is reused
  - If no match is similar enough, a new `canonical_id` is generated using `uuid4`
- The point's payload is updated in Qdrant with:
  - `status: validated`
  - `canonical_id: ...`
  - `processed_at: ...` (timestamp of deduplication)

### Fallback mechanism

If the Qdrant `search()` API fails or returns no candidates:
- The script uses `scroll()` to retrieve all previously validated points
- Cosine similarity is computed manually using `scikit-learn`'s `cosine_similarity` function (for fallback)


### Final output (Qdrant Point Payload)
```jsonc
{
  "payload": {
    "guid": "2021401/https___data.europeana.eu_item_12345",
    "status": "validated or pending",
    "canonical_id": "1f9ef...d2a1",
    "processed_at": "2025-07-13T10:42:00"
  },
  "vectors": {
    "combined": [0.123, 0.456, 0.789   ],  
    "image": [0.101, 0.202, 0.303   ]      
  }
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
- Deduplication is based **only** on image embeddings, as visual content provides a more stable and discriminative signal than the combined one. This helps reduce false positives caused by generic or noisy textual metadata. Metadata only deduplication was already implemented in previous layers.
- No points are deleted from Qdrant: all embeddings remain available for future clustering, recommendation, or analytics tasks

---

##  Future improvements
- Allow points with `status = "failed"` to be retried after a timeout or under manual intervention
- Consider switching or integrating the vector storage to Redis, as it is evolving into a unified data engine supporting vector embeddings and similarity search alongside traditional caching and key-based access. 

---
## Search Quality Evaluation

Qdrant also supports **automated evaluation** of search accuracy through `precision@k`. Tests were run on both `combined` and `image` vectors in the collection on an example run and these below were the results.

#### Results Summary

| Vector Type | Dim  | Distance | Precision@10 | Search Time (avg) |
|-------------|------|----------|---------------|-------------------|
| `combined`  | 1024 | Cosine   | 100% ± 0      | ~50 ms            |
| `image`     | 512  | Cosine   | 100% ± 0      | ~50 ms            |

Each entry such as `precision@10: 1` confirms that:
- For all 100 test points, Qdrant retrieved **all 10 nearest expected neighbors correctly**.
- Both **exact** and **regular (approximate)** search modes show similar latency and performance.

#### Interpretation and considerations

- **Embedding Quality**: Perfect `precision@10 = 1.0 ± 0` suggests excellent vector clustering and alignment.
-  **Speed**: Search times remain consistently under 60ms — fast and production-friendly.
-  **Consistency Across Modalities**: Both `image` and `combined` embeddings perform equally well, showing robustness.
- the 100% precision is a very positive outcome, but it is important to note that in small or homogeneous dataset, achieving perfect results may be easier and less indicative of generalization ability.


This evaluation from Qdrant itself shows that the **vector-based similarity search system** is accurate and efficient, which is the primarily reasons we choose it.

