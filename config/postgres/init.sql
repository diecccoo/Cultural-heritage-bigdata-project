CREATE TABLE IF NOT EXISTS join_metadata_deduplicated (
    id SERIAL PRIMARY KEY,
    guid TEXT,
    user_id TEXT,
    tags TEXT[],
    comment TEXT,
    timestamp TIMESTAMP,
    source TEXT,
    creator TEXT,
    description TEXT,
    edm_rights TEXT,
    format TEXT,
    image_url TEXT[],
    isshownby TEXT[],
    language TEXT,
    provider TEXT,
    subject TEXT[],
    title TEXT,
    type TEXT
);

-- Copia identica per staging
CREATE TABLE IF NOT EXISTS join_metadata_staging (
    id SERIAL PRIMARY KEY,
    guid TEXT,
    user_id TEXT,
    tags TEXT[],
    comment TEXT,
    timestamp TIMESTAMP,
    source TEXT,
    creator TEXT,
    description TEXT,
    edm_rights TEXT,
    format TEXT,
    image_url TEXT[],
    isshownby TEXT[],
    language TEXT,
    provider TEXT,
    subject TEXT[],
    title TEXT,
    type TEXT
);