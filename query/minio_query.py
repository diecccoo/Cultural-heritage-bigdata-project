from pyspark.sql import SparkSession

# 1. Inizializzazione della SparkSession
# (Non è necessario passare le configurazioni S3 qui se le imposti dopo via hadoopConfiguration())
spark = SparkSession.builder \
    .appName("MinioDeltaQuery") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Configurazione di Hadoop per l'accesso a MinIO
# Questa è la parte che hai fornito e che è cruciale per connettersi a MinIO.
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000") # Assicurati che 'minio' sia raggiungibile (es. nome servizio in Docker/Kubernetes, o IP)
hadoopConf.set("fs.s3a.access.key", "minio")
hadoopConf.set("fs.s3a.secret.key", "minio123")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 3. Esecuzione della Query SQL sulla Delta Table
# Ora puoi interrogare la tua Delta table su MinIO.
sql_query = """
SELECT
    (SUM(CASE WHEN title IS NULL OR TRIM(title) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_title,
    (SUM(CASE WHEN guid IS NULL OR TRIM(guid) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_guid,
    (SUM(CASE WHEN image_url IS NULL OR TRIM(image_url) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_image_url,
    (SUM(CASE WHEN timestamp_created IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_timestamp_created,
    (SUM(CASE WHEN provider IS NULL OR TRIM(provider) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_provider,
    (SUM(CASE WHEN description IS NULL OR TRIM(description) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_description,
    (SUM(CASE WHEN creator IS NULL OR TRIM(creator) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_creator,
    (SUM(CASE WHEN subject IS NULL OR TRIM(subject) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_subject,
    (SUM(CASE WHEN language IS NULL OR TRIM(language) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_language,
    (SUM(CASE WHEN type IS NULL OR TRIM(type) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_type,
    (SUM(CASE WHEN format IS NULL OR TRIM(format) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_format,
    (SUM(CASE WHEN rights IS NULL OR TRIM(rights) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_rights,
    (SUM(CASE WHEN dataProvider IS NULL OR TRIM(dataProvider) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_dataProvider,
    (SUM(CASE WHEN isShownAt IS NULL OR TRIM(isShownAt) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_isShownAt,
    (SUM(CASE WHEN edm_rights IS NULL OR TRIM(edm_rights) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_missing_edm_rights
FROM delta.`s3a://heritage/cleansed/europeana`
"""

result_df = spark.sql(sql_query)

# Stampa l'header delle colonne
print("Query Results:")
print("-" * 50)
print(", ".join(result_df.columns))
print("-" * 50)

# Raccogli le righe del DataFrame e stampale
# Attenzione: .collect() porta tutti i dati nel driver, usarlo con cautela su grandi dataset.
for row in result_df.collect():
    print(row.asDict()) # Stampa la riga come un dizionario per una migliore leggibilità

print("-" * 50)

# 4. Ferma la SparkSession
spark.stop()