import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import time

# paho: È la libreria Python ufficiale per connettersi a broker MQTT. Ti permette di: connetterti a Mosquitto, 
# pubblicare e sottoscrivere a topic, gestire messaggi in tempo reale

# Configurazione
MQTT_BROKER = "localhost"      # o 'mqtt' se sei in container
MQTT_PORT = 1883
MQTT_TOPIC = "heritage/annotations"

KAFKA_BROKER = "localhost:9092"  # o 'kafka:9092' se sei in container
KAFKA_TOPIC = "heritage_annotations"


# Attendi qualche secondo prima di connetterti a Kafka
print("⏳ Attendo Kafka...")
time.sleep(10)

# Ora crea il producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)


# Callback: quando arriva un messaggio MQTT
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"📩 Ricevuto da MQTT: {payload}")
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"📤 Inviato a Kafka su topic `{KAFKA_TOPIC}`")
    except Exception as e:
        print(f"❌ Errore: {e}")

# Setup client MQTT
mqtt_client = mqtt.Client(protocol=mqtt.MQTTv311)
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.subscribe(MQTT_TOPIC)
mqtt_client.on_message = on_message

print(f"🔄 In ascolto su MQTT `{MQTT_TOPIC}` → Kafka `{KAFKA_TOPIC}` ...")
mqtt_client.loop_forever()
