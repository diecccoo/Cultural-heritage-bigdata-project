#!/bin/bash

# Script per creare automaticamente i topic Kafka
# Usato nel container 'kafka-init'

# KAFKA_BROKER="kafka:9092,kafka2:9093,kafka3:9094" # CON 3 BROKER (kafka 3)
KAFKA_BROKER="kafka:9092,kafka2:9093" # CON 2 BROKER (kafka 2)

# Funzione per creare un topic con nome, partizioni e repliche
create_topic() {
  TOPIC=$1
  PARTITIONS=$2
  REPLICAS=$3

  echo "Creating topic '$TOPIC' with $PARTITIONS partitions and $REPLICAS replicas..."

  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server $KAFKA_BROKER \
    --create \
    --if-not-exists \
    --topic $TOPIC \
    --partitions $PARTITIONS \
    --replication-factor $REPLICAS 
}

# Elenco dei topic da creare (puoi aggiungerne altri qui)
# create_topic "nome_topic" numero_partizioni numero_repliche
create_topic "user_annotations" 3 2
create_topic "europeana_metadata" 3 2


echo " Waiting for Kafka to become ready..."


# Attendi che i topic siano creati
echo "Waiting for topics to be created..."
until /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list >/dev/null 2>&1; do
  sleep 5
done      
# Verifica la creazione dei topic
echo "Listing all topics..."
/opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server $KAFKA_BROKER \
  --list    


