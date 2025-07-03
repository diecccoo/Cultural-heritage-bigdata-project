import time
import os

INTERVAL_MINUTES = 30  # Cambia qui per modificare la frequenza

while True:
    print("[Scheduler] Avvio job Spark di cleansing metadati Europeana...")

    exit_code = os.system(
        "spark-submit "
        "--master local[*] "
        "--packages io.delta:delta-spark_2.12:3.3.1 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "metadata_to_delta_table.py"
    )

    if exit_code == 0:
        print(f"[Scheduler] ✅ Job completato. Attendo {INTERVAL_MINUTES} minuti...\n")
    else:
        print("[Scheduler] ❌ ERRORE durante il job Spark. Riprovo tra poco...\n")

    time.sleep(INTERVAL_MINUTES * 60)
