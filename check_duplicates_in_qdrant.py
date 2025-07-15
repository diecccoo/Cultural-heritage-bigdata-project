import requests
from collections import defaultdict, Counter

url = "http://localhost:6333/collections/heritage_embeddings/points/scroll"

def scroll_all_points():
    points = []
    offset = None
    while True:
        payload = {
            "limit": 100,
            "with_payload": True,
        }
        if offset:
            payload["offset"] = offset
        res = requests.post(url, json=payload)
        res.raise_for_status()
        data = res.json()["result"]
        points.extend(data["points"])
        offset = data.get("next_page_offset")
        if not offset:
            break
    return points

points = scroll_all_points()

status_counter = Counter()
embedding_status_counter = Counter()
canonical_id_counter = defaultdict(list)

for p in points:
    payload = p.get("payload", {})
    status = payload.get("status", "unknown")
    embedding_status = payload.get("embedding_status", "unknown")
    canonical_id = payload.get("canonical_id")
    object_id = payload.get("id_object")

    status_counter[status] += 1
    embedding_status_counter[embedding_status] += 1

    if canonical_id:
        canonical_id_counter[canonical_id].append(object_id)

# Analisi gruppi duplicati
group_sizes = [len(lst) for lst in canonical_id_counter.values() if len(lst) > 1]
group_size_dist = Counter(group_sizes)
canonical_total = len(canonical_id_counter)
canonical_unique = sum(1 for lst in canonical_id_counter.values() if len(lst) == 1)
canonical_duplicates = canonical_total - canonical_unique

print("\n📊 METRICHE SU QDRANT:\n")
print(f"• Totale punti                 : {len(points)}")
print(f"• Embedding status            : {dict(embedding_status_counter)}")
print(f"• Status (dedup)              : {dict(status_counter)}")
print(f"• Con canonical_id            : {sum(1 for p in points if p.get('payload', {}).get('canonical_id'))}")
print(f"• Gruppi canonical_id totali  : {canonical_total}")
print(f"• Gruppi duplicati (≥2)       : {canonical_duplicates}")
print(f"• Canonical ID unici          : {canonical_unique}")
print("• Distribuzione gruppi duplicati:")
for size, count in sorted(group_size_dist.items()):
    print(f"   - Gruppi da {size} oggetti: {count}")
