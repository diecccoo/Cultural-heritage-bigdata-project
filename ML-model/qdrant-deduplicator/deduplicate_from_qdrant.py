import os
import time
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, ScrollRequest, NamedVector
from qdrant_client.http.exceptions import UnexpectedResponse
from tqdm import tqdm

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


# ========== CONFIGURAZIONE ==========
COLLECTION_NAME = "heritage_embeddings"
BATCH_SIZE = 100  # Numero di punti da processare per batch
SIMILARITY_THRESHOLD = 0.98  # Soglia di similarità per la deduplicazione
SLEEP_SECONDS = 5  # Secondi di attesa tra i batch
MAX_SEARCH_RESULTS = 1000  # Massimo numero di risultati per ricerca similarity

# ========== INIZIALIZZA QDRANT ==========
client = QdrantClient(host="qdrant", port=6333)

def get_pending_points(limit: int = 1000, offset: Optional[str] = None) -> tuple:
    """
    Recupera punti con status 'pending' dalla collezione.
    Restituisce (points, next_page_offset)
    """
    # Step 1: recupera punti pending usando scroll()
    print("[INFO] Recupero punti 'pending'...")
    
    # Usa scroll() invece di search() per recuperare punti filtrati
    results, next_page_offset = client.scroll(
        collection_name= COLLECTION_NAME,
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
        # vectors=["embedding_image"]
    )
    pending_points = results

    print(f"[INFO] Trovati {len(pending_points)} punti pending.")
    return results, next_page_offset
   

def search_similar_validated_points(image_embedding: List[float], limit: int = MAX_SEARCH_RESULTS) -> List[Dict[str, Any]]:
    """
    Cerca punti simili con status 'validated' usando l'embedding dell'immagine.
    Versione compatibile con qdrant-client 1.6.4
    """
    try:
        filter_condition = Filter(
            must=[
                FieldCondition(
                    key="status",
                    match=MatchValue(value="validated")
                )
            ]
        )
        
        # Per la versione 1.6.4, dobbiamo passare il vettore direttamente
        # e specificare quale vettore usare tramite il parametro vector
        search_results = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=NamedVector(name="embedding_image", vector=image_embedding),
            query_filter=filter_condition,
            limit=limit,
            with_payload=True,
            with_vectors=False,  # Non serve il vettore nei risultati
        )
        
        return search_results
        
    except Exception as e:
        print(f"[ERROR] Errore nella ricerca di similarità: {e}")
        return []

def search_similar_validated_points_fallback(image_embedding: List[float], limit: int = MAX_SEARCH_RESULTS) -> List[Dict[str, Any]]:
    """
    Versione fallback che usa scroll per recuperare tutti i punti validated
    e calcola la similarità manualmente (più lenta ma più compatibile)
    """
    try:
        
        
        print("[FALLBACK] Usando metodo fallback per ricerca similarità...")
        
        # Recupera tutti i punti validated
        filter_condition = Filter(
            must=[
                FieldCondition(
                    key="status",
                    match=MatchValue(value="validated")
                )
            ]
        )
        
     
        all_validated_points = []
        offset = None
        
        while True:
            response = client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=scroll_request.filter,
                limit=scroll_request.limit,
                offset=offset,
                with_payload=scroll_request.with_payload,
                with_vectors=scroll_request.with_vectors
            )
            
            points = response[0] if response else []
            offset = response[1] if len(response) > 1 else None
            
            all_validated_points.extend(points)
            
            if not offset or len(points) == 0:
                break
        
        # Calcola similarità manualmente
        results = []
        query_vector = np.array(image_embedding).reshape(1, -1)
        
        for point in all_validated_points:
            if hasattr(point, 'vector') and point.vector:
                # Gestisci vettori multipli
                if isinstance(point.vector, dict):
                    target_vector = point.vector.get('embedding_image')
                else:
                    target_vector = point.vector
                
                if target_vector:
                    target_vector = np.array(target_vector).reshape(1, -1)
                    similarity = cosine_similarity(query_vector, target_vector)[0][0]
                    
                    # Simula la struttura del risultato di search
                    result = {
                        'id': point.id,
                        'score': similarity,
                        'payload': point.payload
                    }
                    results.append(result)
        
        # Ordina per score decrescente e limita i risultati
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:limit]
        
    except Exception as e:
        print(f"[ERROR] Errore nel metodo fallback: {e}")
        return []

def get_or_create_canonical_id(similar_points: List[Dict[str, Any]], similarity_threshold: float = SIMILARITY_THRESHOLD) -> str:
    """
    Determina il canonical_id da assegnare al punto.
    Se trova un punto simile sopra la soglia, usa il suo canonical_id.
    Altrimenti crea un nuovo canonical_id.
    """
    for result in similar_points:
        try:
            # Se è un dict (fallback)
            score = result['score']
            payload = result.get('payload', {})
        except TypeError:
            # Se è un oggetto ScoredPoint
            score = result.score
            payload = result.payload

        if score >= similarity_threshold:
            canonical_id = payload.get('canonical_id')
            if canonical_id:
                print(f"[DEDUP] Trovato duplicato con similarità {score:.4f}, canonical_id: {canonical_id}")
                return canonical_id

    
    # Nessun duplicato trovato, crea nuovo canonical_id
    new_canonical_id = str(uuid.uuid4())
    print(f"[DEDUP] Nessun duplicato trovato, nuovo canonical_id: {new_canonical_id}")
    return new_canonical_id

def update_point_status(point_id: str, canonical_id: str, payload: Dict[str, Any]) -> bool:
    """
    Aggiorna il punto con status 'validated' e canonical_id
    """
    try:
        # Aggiorna il payload
        updated_payload = payload.copy()
        updated_payload['status'] = 'validated'
        updated_payload['canonical_id'] = canonical_id
        updated_payload['processed_at'] = datetime.now().isoformat()
        
        # Aggiorna il punto
        client.set_payload(
            collection_name=COLLECTION_NAME,
            payload=updated_payload,
            points=[point_id]
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Errore nell'aggiornamento del punto {point_id}: {e}")
        return False

def deduplicate_batch(points: List[Any]) -> int:
    """
    Processa un batch di punti per la deduplicazione
    """
    processed_count = 0
    
    for point in tqdm(points, desc="Processando batch"):
        try:
            point_id = point.id
            payload = point.payload or {}
            
            # Verifica che il punto abbia l'embedding dell'immagine
            if not hasattr(point, 'vector') or not point.vector:
                print(f"[SKIP] Punto {point_id} senza vettore")
                continue
            
            # Gestisci vettori multipli
            if isinstance(point.vector, dict):
                image_embedding = point.vector.get('embedding_image')
            else:
                image_embedding = point.vector
            
            if not image_embedding:
                print(f"[SKIP] Punto {point_id} senza embedding_image")
                continue
            
            # Cerca punti simili tra quelli validated
            similar_points = search_similar_validated_points(image_embedding)
            
            # Se la ricerca principale fallisce, usa il fallback
            if not similar_points:
                similar_points = search_similar_validated_points_fallback(image_embedding)
            
            # Determina il canonical_id
            canonical_id = get_or_create_canonical_id(similar_points, SIMILARITY_THRESHOLD)
            
            # Aggiorna il punto
            if update_point_status(point_id, canonical_id, payload):
                processed_count += 1
                print(f"[SUCCESS] Processato punto {point_id}")
            else:
                print(f"[FAILED] Errore nel processamento punto {point_id}")
                
        except Exception as e:
            print(f"[ERROR] Errore nel processamento punto {point.id}: {e}")
            continue
    
    return processed_count

def main():
    """
    Main loop per la deduplicazione
    """
    print(f"[INFO] Avvio deduplicazione - {datetime.now().isoformat()}")
    print(f"[INFO] Configurazione: BATCH_SIZE={BATCH_SIZE}, SIMILARITY_THRESHOLD={SIMILARITY_THRESHOLD}")
    
    total_processed = 0
    offset = None
    
    while True:
        try:
            # Recupera punti pending
            pending_points, next_offset = get_pending_points(BATCH_SIZE, offset)
            
            if not pending_points:
                print(f"[INFO] Nessun punto pending trovato. Totale processati: {total_processed}")
                if offset is None:  # Prima iterazione senza risultati
                    print(f"[INFO] Attendo {SLEEP_SECONDS} secondi prima del prossimo controllo...")
                    time.sleep(SLEEP_SECONDS)
                    continue
                else:  # Finito il batch corrente
                    print("[INFO] Completata deduplicazione del batch corrente")
                    break
            
            print(f"[INFO] Trovati {len(pending_points)} punti pending.")
            
            # Processa il batch
            batch_processed = deduplicate_batch(pending_points)
            total_processed += batch_processed
            
            print(f"[INFO] Batch completato: {batch_processed}/{len(pending_points)} punti processati")
            print(f"[INFO] Totale processati: {total_processed}")
            
            # Aggiorna offset per il prossimo batch
            offset = next_offset
            
            # Pausa tra i batch
            if offset:
                print(f"[INFO] Pausa di {SLEEP_SECONDS} secondi prima del prossimo batch...")
                time.sleep(SLEEP_SECONDS)
        
        except Exception as e:
            print(f"[ERROR] Errore nel main loop: {e}")
            print(f"[INFO] Attendo {SLEEP_SECONDS} secondi prima di riprovare...")
            time.sleep(SLEEP_SECONDS)
    
    print(f"[INFO] Deduplicazione completata. Totale punti processati: {total_processed}")

if __name__ == "__main__":
    main()