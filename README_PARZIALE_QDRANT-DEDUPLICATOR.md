
# 🧠 Qdrant Deduplication Pipeline

Questo container esegue la **deduplicazione degli oggetti culturali** presenti in Qdrant tramite similarità tra embedding di immagini. 

## 🎯 Obiettivo

Identificare duplicati semantici tra oggetti simili (es. stessa foto caricata da due archivi diversi), assegnando un `canonical_id` comune e aggiornando lo `status` da `"pending"` a `"validated"`.

## ⚙️ Funzionamento

### 1. Estrazione da Qdrant

Viene interrogata la collezione `heritage_embeddings` con `scroll()` per recuperare punti con:

```json
{ "status": "pending" }
```

Fino a `BATCH_SIZE` punti per batch.

### 2. Calcolo similarità

Per ogni punto pending:
- Si estrae il vettore `embedding_image`
- Si cercano punti già `"validated"` simili ≥ `SIMILARITY_THRESHOLD` (es. `0.98`) usando Qdrant `search()`

Se fallisce o restituisce zero risultati, viene usata una **versione fallback** che scarica tutti i punti `validated` e calcola la **cosine similarity** manualmente con `scikit-learn`.

### 3. Canonical ID

- Se un punto simile supera la soglia, il suo `canonical_id` viene riusato.
- Altrimenti, viene generato un nuovo UUID.

### 4. Scrittura su Qdrant

Il punto viene aggiornato con:
```json
{
  "status": "validated",
  "canonical_id": "...",
  "processed_at": "2025-07-12T..."
}
```

## 🐳 Docker & Deploy

## 📜 Log di esempio

```
[INFO] Trovati 647 punti pending.
[DEDUP] Nessun duplicato trovato, nuovo canonical_id: 123e...
[SUCCESS] Processato punto 00a7...
```



---

Questo modulo garantisce l’unicità semantica degli oggetti nell'archivio, preparandoli per il clustering e la raccomandazione.
