# versione per mqtt non piu in  uso
import json
import time
import random
import paho.mqtt.client as mqtt

# Config MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "heritage/annotations"

# Oggetti di esempio
objects = ["object1234", "object5678", "object9012"]
tags_pool = ["gotico", "scultura", "legno", "madonna", "colori vivaci", "religioso"]

# Crea messaggio simulato
def generate_fake_annotation():
    return {
        "user_id": f"user{random.randint(1, 100)}",
        "object_id": random.choice(objects),
        "tags": random.sample(tags_pool, k=3),
        "comment": "Annotazione simulata per test",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

# Pubblica
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

msg = generate_fake_annotation()
client.publish(MQTT_TOPIC, json.dumps(msg))
print(f"📤 Pubblicato messaggio simulato:\n{json.dumps(msg, indent=2)}")
client.disconnect()
