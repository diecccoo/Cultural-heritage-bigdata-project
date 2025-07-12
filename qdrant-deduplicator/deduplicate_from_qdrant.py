"""
Deduplicazione asincrona di embeddings immagine su Qdrant.
- Cerca punti con status == "pending"
- Fa KNN su embedding_image contro punti "validated"
- Se simile con sim ≥ 0.98: assegna stesso canonical_id
- Altrimenti: assegna nuovo canonical_id
- Aggiorna il punto su Qdrant con status = "validated"

Requisiti:
- qdrant-client >= 1.6.4
- canonical_id può essere generato via hash o contatore Redis (qui hash di id_object)
"""

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct
import hashlib
from tqdm import tqdm
import time

DEDUPE_INTERVAL_SECONDS = 300  # es. ogni 5 minuti

while True:
    print("[INFO] Avvio ciclo di deduplicazione...")
    COLLECTION = "heritage_embeddings"
    VECTOR_NAME = "embedding_image"
    SIM_THRESHOLD = 0.98
    TOP_K = 3  # top-k vicini tra cui cercare (poi si filtra per soglia)

    client = QdrantClient(url="http://qdrant:6333", prefer_grpc=False)

    # Step 1: recupera punti pending usando scroll()
    print("[INFO] Recupero punti 'pending'...")
    
    # Usa scroll() invece di search() per recuperare punti filtrati
    results, next_page_offset = client.scroll(
        collection_name=COLLECTION,
        scroll_filter=Filter(
            must=[
                FieldCondition(
                    key="status",
                    match=MatchValue(value="pending")
                )
            ]
        ),
        limit=1000,
        with_payload=True,
        with_vectors=True
    )
    pending_points = results

    print(f"[INFO] Trovati {len(pending_points)} punti pending.")

    # Step 2: deduplica ciascun punto
    for point in tqdm(pending_points, desc="Deduplicazione"):

        query_vector = point.vector.get(VECTOR_NAME)
        id_object = point.payload.get("id_object")
        point_id = point.id

        # Esegue KNN tra i validated usando search()
        search_results = client.search(
            collection_name=COLLECTION,
            query_vector=query_vector,  # Rimuovi la tupla, passa direttamente il vettore
            limit=TOP_K,
            search_params={"exact": False},  # Parametro opzionale per performance
            filter=Filter(
                must=[
                    FieldCondition(
                        key="status",
                        match=MatchValue(value="validated")
                    )
                ]
            )
        )

        # Trova match con similarità sopra soglia
        candidate = next((p for p in search_results if p.score >= SIM_THRESHOLD), None)

        if candidate:
            canonical_id = candidate.payload.get("canonical_id")
            print(f"[INFO] Punto {point_id} duplicato di {candidate.id} (canonical_id: {canonical_id})")
        else:
            # Nuovo canonical_id = hash(id_object)
            canonical_id = hashlib.md5(id_object.encode()).hexdigest()
            print(f"[INFO] Punto {point_id} è unico (canonical_id: {canonical_id})")

        # Prepara aggiornamento payload
        new_payload = {
            "canonical_id": canonical_id,
            "status": "validated"
        }

        client.set_payload(
            collection_name=COLLECTION,
            payload=new_payload,
            points=[point_id]
        )

    print(f"[INFO] Deduplicazione completata. Attendo {DEDUPE_INTERVAL_SECONDS} secondi...\n")
    time.sleep(DEDUPE_INTERVAL_SECONDS)