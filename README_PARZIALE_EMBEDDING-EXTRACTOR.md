# Embedding Extractor (CLIP) – Heritage Project

Questo container esegue l'estrazione automatica di **embedding multimodali** (testo + immagine) da metadati culturali salvati su MinIO in formato Delta Lake. È parte della pipeline di analisi per collezioni Europeana/CBL nel progetto *Cultural Heritage Big Data*.

---

## 🔍 Cosa fa questo container

1. **Legge dati** dal layer `cleansed` su MinIO (`s3a://heritage/cleansed/europeana/`) usando Apache Spark.
2. **Estrae testo e immagini** da ciascun record:
   - Combina `title`, `subject`, `creator`, `type`, `description` in un unico testo.
   - Recupera `isShownBy` (URL immagine).
3. **Calcola gli embedding** con il modello `CLIP (openai/clip-vit-base-patch32)` di HuggingFace:
   - Embed sia del testo che dell'immagine, se disponibile.
   - Solo embedding testuale se l'immagine è mancante o invalida.
4. **Scrive i risultati** nel layer `cleansed/embeddings` come DeltaTable, con merge/upsert su `id_object`.

---

## ⚙️ Architettura tecnica

| Componente         | Dettagli |
|--------------------|----------|
| **Spark**          | Lettura e scrittura dati in formato Delta da/su MinIO |
| **MinIO**          | Sistema di storage S3-compatibile, endpoint: `http://minio:9000` |
| **CLIP**           | Modello Transformer multimodale (testo+immagine) da HuggingFace |
| **Transformers**   | Usa `CLIPProcessor` e `CLIPModel` |
| **Delta Lake**     | Merge automatico dei risultati (`merge_embeddings`) |
| **Checkpoints**    | Tiene traccia dell’ultimo `guid` elaborato in `embedding_last_processed.txt` |
| **Batching**       | Elaborazione in batch (fino a 112 record Spark, eventualmente a 16 per GPU) |
| **Error Handling** | Skippa immagini mancanti, fallback in caso di errori embedding |
| **Loop infinito**  | Estrazione periodica ogni 60 secondi (`SLEEP_SECONDS`) |

---

## 📦 File principali

- `extracting_embeddings.py`: script principale di estrazione.
- `Dockerfile`: configura l’ambiente Python, Spark, Delta, HuggingFace.

---

## 🧠 Logiche dietro le scelte

- ⚡ **Batching e RAM**: lo script processa 112 record alla volta per evitare saturazione di RAM.
- 🧠 **Fallback in caso di errore immagine**: se `PIL.Image.open()` fallisce, salva comunque l'embedding testuale.
- 💾 **Salvataggio stato**: l’ultimo GUID processato viene salvato su MinIO per evitare duplicati in cicli successivi.
- 🔁 **Loop continuo**: gira per sempre, estraendo embedding man mano che i dati arrivano in `cleansed`.

---

## 📥 Output finale

Ogni record salvato contiene:

| Campo              | Tipo                | Descrizione                        |
|-------------------|---------------------|------------------------------------|
| `id_object`        | `String`            | GUID univoco del record            |
| `embedding_text`   | `Array[Float]`      | Embedding vettoriale del testo     |
| `embedding_image`  | `Array[Float]` / `None` | Embedding immagine (se presente)   |
| `embedding_status` | `"OK"` / `"FAILED"` / `"NO_IMAGE"` | Stato embedding |

