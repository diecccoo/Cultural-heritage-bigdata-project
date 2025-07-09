# 🧠 DEDUPLICATION PIPELINE – README (sezione parziale)

Questa sezione documenta la pipeline containerizzata per la deduplicazione di oggetti culturali basata su embeddings visivi.  
La pipeline è composta da **tre container indipendenti**, che comunicano tramite un **volume Docker condiviso** (`deduplication-data`).

---

## 📦 Container 1: `embeddings-exporter`

### 🔹 Scopo
Estrae gli embeddings da una DeltaTable salvata su MinIO (`s3a://heritage/cleansed/embeddings/`)  
Filtra solo quelli con `embedding_status == "OK"` e salva un file `embeddings.parquet`.

### 🔹 Output
- `embeddings.parquet` scritto in `/shared-data` (cioè nel volume `deduplication-data`)

### 🔹 Script eseguito
`spark-apps/embeddings-exporter/export_embeddings.py`

### 🔹 Dockerfile
`spark-apps/embeddings-exporter/Dockerfile`

---

## 📦 Container 2: `faiss`

### 🔹 Scopo
Legge `embeddings.parquet`, calcola per ogni embedding immagine i `top-20` più simili con cosine similarity (via Faiss),  
rimuove i self-match e salva il risultato.

### 🔹 Output
- `faiss_output_topk.parquet` scritto in `/shared-data` (volume `deduplication-data`)

### 🔹 Script eseguito
`faiss/faiss_knn.py`

### 🔹 Dockerfile
`faiss/Dockerfile`

---

## 📦 Container 3: `deduplicator-to-cleansed`

### 🔹 Scopo
Legge le coppie generate da Faiss e i vettori originali da Delta Lake,  
calcola la **cosine similarity esatta**, tiene solo le coppie con `cosine_sim ≥ 0.98`,  
e scrive una nuova DeltaTable contenente **solo oggetti unici** (basandosi su `min(id_object)`).

### 🔹 Output
- DeltaTable finale salvata in: `s3a://heritage/cleansed/deduplicated/`

### 🔹 Script eseguito
`spark-apps/deduplicator-to-cleansed/deduplicate_from_faiss_candidates.py`

### 🔹 Dockerfile
`spark-apps/deduplicator-to-cleansed/Dockerfile`

---

## 📁 Volume condiviso: `deduplication-data`

Tutti i container montano il volume come:

```yaml
volumes:
  - deduplication-data:/shared-data
```

Il volume è definito in fondo al `docker-compose.yml`:

```yaml
volumes:
  deduplication-data:
```

---

## ▶️ Esecuzione consigliata (in ordine)

```bash
docker-compose run --rm embeddings-exporter
docker-compose run --rm faiss
docker-compose run --rm deduplicator-to-cleansed
```

## 🤔 Motivazioni delle scelte progettuali

### 🔹 Perché abbiamo usato Faiss (e non solo Spark)?
- Spark ha metodi nativi (`crossJoin`, `LSH`) per confrontare vettori, ma:
  - `crossJoin` è molto pesante computazionalmente.
  - `LSH` è approssimato e non consente una soglia esatta su cosine similarity.
- Faiss è progettato per calcoli efficienti su grandi volumi di embedding e ci consente di:
  - Calcolare i `top-k` vicini per ciascun embedding immagine
  - Normalizzare per usare la cosine similarity come dot product
  - Scalare meglio in termini di prestazioni
- Abbiamo quindi scelto **Faiss + filtro Spark preciso**, per avere:
  - **efficienza** nel calcolo dei candidati
  - **precisione** con la verifica della soglia `cosine_sim ≥ 0.98` in Spark

---

### 🔹 Perché abbiamo salvato file intermedi come Parquet su volume Docker?
- Spark e Faiss lavorano in ambienti separati (Spark in cluster, Faiss in Python puro)
- Abbiamo bisogno di uno scambio dati tra container:
  - Salvare in **Parquet** è leggero, veloce e ben supportato da entrambi
  - Il volume `deduplication-data` è condiviso e garantisce persistenza tra i passaggi
- In alternativa, avremmo dovuto:
  - Scrivere/leggere da MinIO anche con Python (più setup)
  - O passare tutto via Kafka/Redis, che sarebbe stato **eccessivo per un task batch**

---

### 🔹 Perché abbiamo separato i container (3 servizi)?
- Ogni container ha un compito chiaro:
  - `embeddings-exporter`: estrazione da Delta Lake
  - `faiss`: calcolo top-k embedding simili
  - `deduplicator-to-cleansed`: deduplicazione con Spark
- Questa scelta offre:
  - **Modularità**: puoi testare o sostituire ogni parte separatamente
  - **Pulizia architetturale**: ogni container ha uno script, uno scopo, un log
  - **Facilità di orchestrazione** futura (cronjob, Airflow, etc.)

---

### 🔹 Perché abbiamo scelto un volume Docker (`deduplication-data`) e non bind mount?
- Un volume Docker:
  - è **isolato e riproducibile**, non dipende dal filesystem host
  - è più coerente con pratiche **container-first**
  - evita problemi su Windows/macOS legati a permessi o formati file
- Poiché non è necessario ispezionare manualmente i file `.parquet`, abbiamo preferito questo approccio più “pulito” e **orientato alla produzione**

---

### 🔹 Perché abbiamo mantenuto `min(id_object)` come logica per la deduplicazione?
- È una regola semplice, deterministica e facilmente verificabile
- Non introduce arbitrarietà (es: in base a `timestamp`, `hash`, ecc.)
- Evita la necessità di salvare metadati sui duplicati scartati

---

### 🔹 Perché scriviamo la tabella finale in `overwrite`?
- L’intera deduplicazione è batch e completa
- Sovrascrivere evita duplicati o conflitti con run precedenti
- Garantisce che la tabella `cleansed/deduplicated/` rappresenti **lo stato aggiornato**