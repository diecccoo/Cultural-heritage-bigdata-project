# script per testare subito che il tuo broker MQTT sia attivo e che riceva messaggi.

# Un MQTT broker è il centro di smistamento dei messaggi nel protocollo MQTT (Message Queuing Telemetry Transport), un protocollo leggero e veloce, ideale per crowdsourcing
# In pratica:
# I client si connettono al broker:
# Alcuni pubblicano messaggi (publisher)
# Altri ricevono messaggi (subscriber)

# Il broker smista i messaggi ai client interessati, in base al topic.
import paho.mqtt.client as mqtt
import json
import time

# Configurazione
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "heritage/annotations"

# Dato di esempio: metadato annotato da utente
sample_message = {
    "object_id": "object1234",
    "user_id": "user42",
    "tags": ["religious", "wood", "sculpture"],
    "comment": "Statua lignea raffigurante San Giorgio",
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
}

# Serializza JSON
payload = json.dumps(sample_message)

# Crea client MQTT
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Pubblica messaggio
client.publish(MQTT_TOPIC, payload)
print(f"Messaggio pubblicato su topic `{MQTT_TOPIC}`:")
print(payload)

client.disconnect()
