import redis

# Connessione a Redis (hostname = "redis" nel docker-compose)
r = redis.Redis(host='redis', port=6379, decode_responses=True)

# Scrivi una chiave
r.set('greeting', 'hello from Python!')

# Leggi e stampa la chiave
value = r.get('greeting')
print(f"✅ Redis returned: {value}")
