import requests

url = "http://localhost:6333/collections/heritage_embeddings/points/scroll"
payload = {
    "limit": 100
}
headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)
points = response.json()["result"]["points"]

for p in points:
    payload = p.get("payload", {})
    status = payload.get("status")
    if status == "validated":
        print(f"✅ object_id: {payload.get('id_object')}")
        print(f"   canonical_id: {payload.get('canonical_id')}")
        print()
