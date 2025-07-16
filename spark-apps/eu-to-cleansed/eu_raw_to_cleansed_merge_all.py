# ANCORA DA PROVARE!
# file che converte i metadati Europeana in formato Delta su MinIO in modo incrementale
# Questo script:
# - Legge i metadati Europeana in formato JSON da MinIO
# - Pulisce i dati rimuovendo record con GUID nulli o duplicati
# - Utilizza l'operazione MERGE per aggiornare in modo efficiente la tabella Delta,
#   inserendo nuovi record e AGGIORNANDO!! quelli esistenti senza creare duplicati.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
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
print(f"📥 Lettura JSON grezzi da {RAW_PATH}...")
df_source = spark.read.json(RAW_PATH)
print(f"📊 Numero record letti: {df_source.count()}")

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

print(f"🧹 Numero record dopo cleaning nella sorgente: {df_source_clean.count()}")

# ---------------- Scrittura in formato Delta con logica MERGE ----------------
print(f"💾 Inizio operazione di MERGE sulla tabella Delta in {CLEANSED_PATH}...")

# Verifica se la tabella Delta di destinazione esiste già
if DeltaTable.isDeltaTable(spark, CLEANSED_PATH):
    # La tabella esiste, esegui il MERGE
    delta_table = DeltaTable.forPath(spark, CLEANSED_PATH)

    delta_table.alias("target").merge(
        source=df_source_clean.alias("source"),
        condition="target.guid = source.guid"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() # <-- QUI: updateAll per aggiornare tutti i campi

    print("🔄 MERGE completato. Record esistenti aggiornati e nuovi record inseriti.")
else:
    # La tabella non esiste, la creiamo da zero
    print("🟡 Tabella Delta non trovata. Verrà creata una nuova tabella.")
    df_source_clean.write \
        .format("delta") \
        .save(CLEANSED_PATH)
    print("✨ Nuova tabella Delta creata con successo.")

print("✅ Job completato con successo.")
spark.stop()