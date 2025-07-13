CREATE TABLE IF NOT EXISTS join_metadata_deduplicated (
    id_object TEXT PRIMARY KEY,
    user_id TEXT,
    tags TEXT[],
    comment TEXT,
    timestamp TIMESTAMP,
    location TEXT,
    ingestion_time_ugc TIMESTAMP,
    source TEXT,
    creator TEXT,
    dataProvider TEXT[],
    description TEXT,
    edm_rights TEXT,
    format TEXT,
    image_url TEXT[],
    isShownBy TEXT[],
    language TEXT,
    provider TEXT,
    rights TEXT,
    subject TEXT[],
    timestamp_created_europeana TIMESTAMP,
    title TEXT,
    type TEXT,
    joined_at TIMESTAMP
);

-- Copia identica per staging
CREATE TABLE IF NOT EXISTS join_metadata_staging (LIKE join_metadata_deduplicated INCLUDING ALL);