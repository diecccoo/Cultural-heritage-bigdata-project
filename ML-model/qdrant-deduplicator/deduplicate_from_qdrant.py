import time
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, ScrollRequest, NamedVector
from tqdm import tqdm

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


# ========== CONFIGURATION ==========
COLLECTION_NAME = "heritage_embeddings"
BATCH_SIZE = 100  # Number of points to process in each batch
SIMILARITY_THRESHOLD = 0.97  # Threshold for similarity to consider points as duplicates
SLEEP_SECONDS = 5  # seconds to wait between batches
MAX_SEARCH_RESULTS = 1000  # maximum results to return in search queries

# ========== INITIALIZE QDRANT ==========
client = QdrantClient(host="qdrant", port=6333)

def get_pending_points(limit: int = 1000, offset: Optional[str] = None) -> tuple:
    """
    Retrieves points with status ‘pending’ from the collection.
    Returns (points, next_page_offset)
    """

    print("[INFO] Retrieving points 'pending'...")
    
    # Use scroll to retrieve points with status 'pending'
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
    )
    pending_points = results

    print(f"[INFO] Founded {len(pending_points)} pending.")
    return results, next_page_offset
   

def search_similar_validated_points(image_embedding: List[float], limit: int = MAX_SEARCH_RESULTS) -> List[Dict[str, Any]]:
    """
    Search for similar points with status ‘validated’ using image embedding.
    Version compatible with qdrant-client 1.6.4
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
        
        # For version 1.6.4, we need to pass the vector directly
        # and specify which vector to use via the vector parameter
        search_results = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=NamedVector(name="image", vector=image_embedding),
            query_filter=filter_condition,
            limit=limit,
            with_payload=True,
            with_vectors=False,  
        )
        
        return search_results
        
    except Exception as e:
        print(f"[ERROR] Error in similarity: {e}")
        return []

def search_similar_validated_points_fallback(image_embedding: List[float], limit: int = MAX_SEARCH_RESULTS) -> List[Dict[str, Any]]:
    """
    Fallback version that uses scroll to retrieve all validated points
 and calculates similarity manually (slower but more compatible)
 """
    try:
        
        
        print("[FALLBACK] Using fallback method for similarity search...")
        
        # retrieve all points with status 'validated'
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
                scroll_filter=filter_condition,
                limit=limit,
                offset=offset,
                with_payload=True,
                with_vectors=True
            )

            
            points = response[0] if response else []
            offset = response[1] if len(response) > 1 else None
            
            all_validated_points.extend(points)
            
            if not offset or len(points) == 0:
                break
        
        # calculate similarity for each point
        results = []
        query_vector = np.array(image_embedding).reshape(1, -1)
        
        for point in all_validated_points:
            if hasattr(point, 'vector') and point.vector:
                
                if isinstance(point.vector, dict):
                    target_vector = point.vector.get('image')
                else:
                    target_vector = point.vector
                
                if target_vector:
                    target_vector = np.array(target_vector).reshape(1, -1)
                    similarity = cosine_similarity(query_vector, target_vector)[0][0]
                    
                    # simulate ScoredPoint structure
                    result = {
                        'id': point.id,
                        'score': similarity,
                        'payload': point.payload
                    }
                    results.append(result)
        
        # order results by similarity score
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:limit]
        
    except Exception as e:
        print(f"[ERROR] Error in fallback method: {e}")
        return []

def get_or_create_canonical_id(similar_points: List[Dict[str, Any]], similarity_threshold: float = SIMILARITY_THRESHOLD) -> str:
    """
    Determines the canonical_id to assign to the point.
    If it finds a similar point above the threshold, use its canonical_id.
    Otherwise, it creates a new canonical_id.
    """
    for result in similar_points:
        try:
            # If it is a dict (fallback)
            score = result['score']
            payload = result.get('payload', {})
        except TypeError:
            # if ScoredPoint
            score = result.score
            payload = result.payload

        if score >= similarity_threshold:
            canonical_id = payload.get('canonical_id')
            if canonical_id:
                print(f"[DEDUP] Found duplicate with similarity{score:.4f}, canonical_id: {canonical_id}")
                return canonical_id

    
    # no similar points found above the threshold, create a new canonical_id
    new_canonical_id = str(uuid.uuid4())
    print(f"[DEDUP] No duplicate found, new canonical_id: {new_canonical_id}")
    return new_canonical_id

def update_point_status(point_id: str, canonical_id: str, payload: Dict[str, Any]) -> bool:
    """
    Update the dot with status ‘validated’ and canonical_id
    """
    try:
        # update payload
        updated_payload = payload.copy()
        updated_payload['status'] = 'validated'
        updated_payload['canonical_id'] = canonical_id
        updated_payload['processed_at'] = datetime.now().isoformat()
        
        # update point
        client.set_payload(
            collection_name=COLLECTION_NAME,
            payload=updated_payload,
            points=[point_id]
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error in updating the point {point_id}: {e}")
        return False

def deduplicate_batch(points: List[Any]) -> int:
    """
    Processes a batch of points for deduplication
    """
    processed_count = 0
    
    for point in tqdm(points, desc="Processing batch"):
        try:
            point_id = point.id
            payload = point.payload or {}
            
            
            if not hasattr(point, 'vector') or not point.vector:
                print(f"[SKIP] Point {point_id} without vector")
                continue
            
           
            if isinstance(point.vector, dict):
                image_embedding = point.vector.get('image')
            else:
                image_embedding = point.vector
            
            if not image_embedding:
                print(f"[SKIP] Point {point_id} without embeddings image")
                continue
            
          
            similar_points = search_similar_validated_points(image_embedding)
            
            # if no similar points found, use fallback method
            if not similar_points:
                similar_points = search_similar_validated_points_fallback(image_embedding)
            
            
            canonical_id = get_or_create_canonical_id(similar_points, SIMILARITY_THRESHOLD)
            
            # update the point with the canonical_id and status
            if update_point_status(point_id, canonical_id, payload):
                processed_count += 1
                print(f"[SUCCESS] Processed point {point_id}")
            else:
                print(f"[FAILED] Error in point processing {point_id}")
                
        except Exception as e:
            print(f"[ERROR] Error in point processing {point.id}: {e}")
            continue
    
    return processed_count

def main():
    """
    Main loop 
    """
    print(f"[INFO] Starting deduplication - {datetime.now().isoformat()}")
    print(f"[INFO] Configuration: BATCH_SIZE={BATCH_SIZE}, SIMILARITY_THRESHOLD={SIMILARITY_THRESHOLD}")
    
    total_processed = 0
    offset = None
    
    while True:
        try:
            # retrieve pending points
            pending_points, next_offset = get_pending_points(BATCH_SIZE, offset)
            
            if not pending_points:
                print(f"[INFO] No pending items found. Total processed:{total_processed}")
                if offset is None:  
                    print(f"[INFO] Waiting {SLEEP_SECONDS} seconds before the next check...")
                    time.sleep(SLEEP_SECONDS)
                    continue
                else:  
                    print("[INFO] Completed deduplication of current batch")
                    break
            
            print(f"[INFO] Found {len(pending_points)} point pending.")
            
            # process the batch
            batch_processed = deduplicate_batch(pending_points)
            total_processed += batch_processed
            
            print(f"[INFO] Batch completated: {batch_processed}/{len(pending_points)} points processed")
            print(f"[INFO] Total processed: {total_processed}")
            
            # update offset for the next batch
            offset = next_offset
            
            # wait before the next batch
            if offset:
                print(f"[INFO] Waits for {SLEEP_SECONDS} seconds before next batch...")
                time.sleep(SLEEP_SECONDS)
        
        except Exception as e:
            print(f"[ERROR] Error in the main loop: {e}")
            print(f"[INFO] Waiting {SLEEP_SECONDS} seconds before retrying...")
            time.sleep(SLEEP_SECONDS)
    
    print(f"[INFO] Deduplication completed. Total points processed: {total_processed}")

if __name__ == "__main__":
    main()