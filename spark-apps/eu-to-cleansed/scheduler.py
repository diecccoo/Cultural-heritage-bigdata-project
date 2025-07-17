import time
import os

INTERVAL_MINUTES = 1  

while True:
    print("[Scheduler] Starting Spark job for cleansing Europeana metadata...")

    exit_code = os.system(
        "spark-submit "
        "--master local[*] "
        "--packages io.delta:delta-spark_2.12:3.3.1 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "eu_raw_to_cleansed_merge.py"
    )

    if exit_code == 0:
        print(f"[Scheduler] Job completed successfully. Waiting for {INTERVAL_MINUTES} minutes...\n")
    else:
        print("[Scheduler] ERROR occurred during Spark job. Retrying shortly...\n")

    time.sleep(INTERVAL_MINUTES * 60)
