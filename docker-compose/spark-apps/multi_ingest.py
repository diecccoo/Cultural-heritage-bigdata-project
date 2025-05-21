import subprocess


with open("queries.txt", "r") as f:
    queries = [line.strip() for line in f if line.strip()]

for topic in queries:
    print(f"Ingesting topic: {topic}")
    subprocess.run(["python", "europeana_ingest.py", topic])