import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time

# paho: È la libreria Python ufficiale per connettersi a broker MQTT. Ti permette di: connetterti a Mosquitto, 
# pubblicare e sottoscrivere a topic, gestire messaggi in tempo reale

# Configurazione
MQTT_BROKER = "mqtt"      
# "localhost" se Stai eseguendo fuori da Docker, su Windows (es. da VS Code o PowerShell):
# o 'mqtt' se stai eseguendo dentro un container Docker dello stesso docker-compose.yml (es. bridge.py o mqtt-simulator)
MQTT_PORT = 1883
MQTT_TOPIC = "heritage/annotations"

KAFKA_BROKER = "kafka:9092"  # o 'localhost:9092' se non sei in container
KAFKA_TOPIC = "user_annotations"


# Attendi qualche secondo prima di connetterti a Kafka
print("Attendo Kafka...")
for _ in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers = ['kafka:9092', 'kafka2:9093', 'kafka3:9094'],
            #  parla con tutti e tre i broker Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka è raggiungibile.")
        break
    except NoBrokersAvailable:
        print("Kafka non disponibile, ritento tra 5 secondi...")
        time.sleep(5)
else:
    raise RuntimeError("Kafka non disponibile dopo 10 tentativi")


# Callback: quando arriva un messaggio MQTT
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"Ricevuto da MQTT: {payload}")
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"Inviato a Kafka su topic `{KAFKA_TOPIC}`")
    except Exception as e:
        print(f"Errore: {e}")

# Setup client MQTT
mqtt_client = mqtt.Client(protocol=mqtt.MQTTv311)
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.subscribe(MQTT_TOPIC)
mqtt_client.on_message = on_message

print(f"In ascolto su MQTT `{MQTT_TOPIC}` → Kafka `{KAFKA_TOPIC}` ...")
mqtt_client.loop_forever()
