# 📚 Cultural Heritage Big Data Pipeline

## 1. Overview
> Breve introduzione al progetto: obiettivo, ambito applicativo, tecnologie principali.

This project aims to design and prototype a big data system that supports the digitization, analysis, and exploration of large-scale cultural heritage collections. These include artifacts, manuscripts, images, and user-contributed metadata from museums, libraries, and historical archives.

This project sets up a full data pipeline for collecting, ingesting, processing, and serving cultural heritage data. It simulates users interaction , ingestion to Kafka, and processing with Spark to MinIO, utilized as a data lake with Delta Lake Architecture. 
This system performs analytics, deduplication and recommendations, and it also enables easy exploration and retrieval of cultural content.
---

## 2. System Architecture

### 2.1 Layered Architecture Overview
> Diagramma logico dei sei layer (Ingestion → Serving) con una breve descrizione per ciascuno.

### 2.2 Data Flow Diagram
> Rappresentazione visiva del flusso end-to-end dei dati.

### 2.3 File structure

lll
---

## 3. Data Sources

### 3.1 Europeana Metadata
> Descrizione delle query, campi chiave, struttura JSON.

### 3.2 User Annotations (Synthetic)
> Come vengono generate, struttura dei messaggi, scopo.
iujn
---

## 4. Technologies Used
> Tabella o elenco con: tecnologia – ruolo – motivazione.
iojioj
---

## 5. Pipeline Stages

### 5.1 Ingestion Layer
- Kafka producers
- Europeana API fetch
- Annotation generator

### 5.2 Raw Storage
- Salvataggio JSON su MinIO
- Strategie di partizionamento e batching

### 5.3 Cleansing Layer
- Validazione
- Normalizzazione
- Output in Delta Lake

### 5.4 Machine Learning Model
- CLIP embedding (image + text)
- Salvataggio su Qdrant
- Deduplicazione semantica

### 5.5 Join to Curated Layer
- Join tra oggetti Europeana e annotazioni
- Uso di canonical_id
- Output finale (Delta + PostgreSQL)

### 5.6 Serving Layer
- PostgreSQL per dashboard e query
- Qdrant per recommendation
- Streamlit UI
min
---

## 6. How to Run

### 6.1 Docker Compose Setup
> Spiegazione dei servizi, volumi, reti.

### 6.2 Environment Variables
> API keys, endpoint, accesso a MinIO/Postgres/Qdrant.

### 6.3 Build & Run Commands
> Comandi principali per avviare il sistema.
yhbh
---

## 7. Example Usage

### 7.1 Dashboard Features
> Esplorazione, filtri, similar images, dettagli.

### 7.2 Query Examples
> Query su PostgreSQL (es. filtra per autore)  
> Query su Qdrant (es. simili per embedding)
btgb
---

## 8. Results & Evaluation
> Dati sulla deduplicazione, numero oggetti ingestiti, performance.
iuhy
---
## 9. Lessons Learned
> Cosa ha funzionato, cosa no, insight tecnici.
hjù
---
## 10. Limitations & Future Work
> Cosa manca, cosa può essere migliorato, estensioni possibili.
fghjk
---
## 11. References & Acknowledgments
> Fonti tecniche, API, paper, ringraziamenti.
gfhj
---

## 12. Authors & Contact
> Nomi, email, corso, anno, docente.
fghj
---
