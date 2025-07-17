# funziona
# file che converte i metadati Europeana in formato Delta su MinIO in modo incrementale
# Questo script:
# - Legge i metadati Europeana in formato JSON da MinIO
# - Pulisce i dati rimuovendo record con GUID nulli o duplicati
# - Utilizza l'operazione MERGE per aggiornare in modo efficiente la tabella Delta,
#   IGNORANDO!! i record esistenti e inserendo solo i nuovi.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, countDistinct 
from delta.tables import DeltaTable # Importazione necessaria per MERGE

# ---------------- SparkSession configurata per MinIO + Delta ----------------
spark = SparkSession.builder \
    .appName("CleansUGC Raw JSON to Cleansed Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# --- Percorsi per sorgente e destinazione ---
RAW_PATH = "s3a://heritage/raw/metadata/europeana_metadata/"
CLEANSED_PATH = "s3a://heritage/cleansed/europeana/"

# ---------------- Lettura dei JSON grezzi Europeana ----------------
print(f"Reading JSON from {RAW_PATH}...")
df_source = spark.read.json(RAW_PATH)
print(f"Record read: {df_source.count()}")

# ---------------- Data cleansing ----------------
# Questo DataFrame rappresenta i dati sorgente da "fondere" nella tabella di destinazione
df_source_clean = df_source \
    .filter(col("guid").isNotNull()) \
    .filter(col("image_url").isNotNull()) \
    .dropDuplicates(["guid"])

# Trasforma le stringhe vuote "" in null.
fields_to_clean = [
    "title", "description", "timestamp_created", "provider", "creator", "subject", "language", "type",
    "format", "rights", "dataProvider", "isShownAt", "edm_rights"
]
for field in fields_to_clean:
    df_source_clean = df_source_clean.withColumn(
        field,
        when(col(field) == "", lit(None)).otherwise(col(field))
    )

print(f"Record after cleaning: {df_source_clean.count()}")

# ---------------- Scrittura in formato Delta con logica MERGE ----------------
print(f"Start MERGE operation on Delta table in {CLEANSED_PATH}...")

# Verifica se la tabella Delta di destinazione esiste già
if DeltaTable.isDeltaTable(spark, CLEANSED_PATH):
    # La tabella esiste, esegui il MERGE
    delta_table = DeltaTable.forPath(spark, CLEANSED_PATH)

    delta_table.alias("target").merge(
        source=df_source_clean.alias("source"),
        condition="target.guid = source.guid"
    ).whenNotMatchedInsertAll().execute() 

    print("MERGE completed.")
else:
    print("Delta table not found. Creating a new table.")
    df_source_clean.write \
        .format("delta") \
        .save(CLEANSED_PATH)
    print("New Table created")

print("Job Completed.")

# --- INIZIO BLOCCO DI VERIFICA E DEBUG ---
try:
    print("DEBUG: entered in the try section")
    
    print(f"DEBUG: trying to read Delta table from {CLEANSED_PATH}")
    df_final = spark.read.format("delta").load(CLEANSED_PATH)
    print("DEBUG: reading completed.")

    print("DEBUG: Trying to count unique guids.")
    num_unique_guids = df_final.agg(countDistinct("guid")).collect()[0][0]
    print(f"Number of unique guid in Delta table: {num_unique_guids}")
    print("DEBUG: Guid count completed.")

    if df_final.count() == num_unique_guids:
        print("Didn't find any duplicate based on GUID!")
    else:
        print("WARNING: found duplicates based on GUID!")

except Exception as e:
    print(f"ERROR IN THE DEBUG PART: {e}")
    # Stampa lo stack trace completo per debugging approfondito
    import traceback
    traceback.print_exc()


spark.stop() 
