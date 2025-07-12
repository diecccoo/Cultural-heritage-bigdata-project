import faiss
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pyarrow import Table

# -------------------------------------
# Configurazione
K = 20
input_path = "/shared-data/embeddings.parquet"
output_path = "/shared-data/faiss_output_topk.parquet"

# -------------------------------------
# Lettura degli embeddings
print("📥 Leggo gli embedding da Parquet...")
table = pq.read_table(input_path)
df = table.to_pandas()

# -------------------------------------
# Preparazione dati
print("🔄 Estraggo id e vettori immagine...")
ids = df["id_object"].values
vectors = np.stack(df["embedding_image"].values).astype("float32")

# -------------------------------------
# Normalizzazione (cosine similarity = dot product su vettori L2-normalizzati)
print("🧪 Normalizzazione L2...")
faiss.normalize_L2(vectors)

# -------------------------------------
# Creazione indice Faiss
print("⚙️ Costruzione indice Faiss (IndexFlatIP)...")
index = faiss.IndexFlatIP(vectors.shape[1])
index.add(vectors)

# -------------------------------------
# Ricerca dei vicini più simili
print("🔍 Calcolo top", K, "vicini per ogni oggetto...")
similarities, indices = index.search(vectors, K)

# -------------------------------------
# Costruzione del dataframe con coppie (id_a, id_b, sim)
print("🧱 Costruzione delle coppie (id_a, id_b, sim)...")
results = []

for i, neighbors in enumerate(indices):
    id_a = ids[i]
    for j, idx_b in enumerate(neighbors):
        id_b = ids[idx_b]
        if id_a != id_b:  # Rimuove self-match
            sim = float(similarities[i][j])
            results.append((id_a, id_b, sim))

results_df = pd.DataFrame(results, columns=["id_a", "id_b", "faiss_sim"])

# -------------------------------------
# Salvataggio in Parquet
print(f"💾 Scrivo {len(results_df)} coppie simili in: {output_path}")
table_out = Table.from_pandas(results_df)
pq.write_table(table_out, output_path)

print("✅ Faiss completato.")
