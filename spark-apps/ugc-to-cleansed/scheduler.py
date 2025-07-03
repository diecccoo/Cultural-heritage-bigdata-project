import time
import os

INTERVAL_MINUTES = 5  # Modifica se vuoi un'altra frequenza

while True:
    print("[Scheduler] Avvio job Spark per pulizia e Parquet...")

    exit_code = os.system(
        "spark-submit "
        "--master local[*] "
        "--packages io.delta:delta-spark_2.12:3.3.1 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "/app/ugc_raw_to_cleansed.py"
    )

    if exit_code == 0:
        print(f"[Scheduler] Job completato. Aspetto {INTERVAL_MINUTES} minuti...\n")
    else:
        print("[Scheduler] ERRORE durante il job Spark. Ritento tra poco...\n")

    time.sleep(INTERVAL_MINUTES * 60)
