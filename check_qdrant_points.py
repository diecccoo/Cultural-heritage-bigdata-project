from qdrant_client import QdrantClient

# Connetti al servizio Qdrant
client = QdrantClient(url="http://localhost:6333")

# Conta TUTTI i punti nella collezione (exact=True per conteggio preciso)
count_result = client.count(
    collection_name="heritage_embeddings",
    exact=True
)

print(f"🔢 Totale punti in Qdrant: {count_result.count}")
