-- CREATE TABLE IF NOT EXISTS europeana_items (
--     guid TEXT PRIMARY KEY,
--     title TEXT,
--     creator TEXT,
--     image_url TEXT,
--     timestamp_created TEXT,
--     provider TEXT,
--     description TEXT,
--     subject TEXT,
--     language TEXT,
--     type TEXT,
--     format TEXT,
--     rights TEXT,
--     dataProvider TEXT,
--     isShownAt TEXT,
--     isShownBy TEXT,
--     edm_rights TEXT,
--     raw_json JSONB
-- );

-- CREATE TABLE IF NOT EXISTS ugc_annotations (
--     id SERIAL PRIMARY KEY,
--     object_guid TEXT REFERENCES europeana_items(guid),
--     user_id TEXT,
--     comment TEXT,
--     tags TEXT[],
--     ugc_timestamp TIMESTAMP,
--     location TEXT
-- );


CREATE TABLE IF NOT EXISTS join_metadata_deduplicated (
    id_object TEXT PRIMARY KEY,
    user_id TEXT,
    tags TEXT[],
    comment TEXT,
    timestamp TIMESTAMP,
    location TEXT,
    ingestion_time TIMESTAMP,
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
    timestamp_created TIMESTAMP,
    title TEXT,
    type TEXT,
    joined_at TIMESTAMP
);

