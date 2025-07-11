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


CREATE TABLE IF NOT EXISTS join_metadata (
    id_object TEXT PRIMARY KEY,
    title TEXT,
    creator TEXT,
    description TEXT,
    image_url TEXT,
    comment TEXT,
    user_id TEXT,
    annotation_timestamp TIMESTAMP,
    tags TEXT[]
);
