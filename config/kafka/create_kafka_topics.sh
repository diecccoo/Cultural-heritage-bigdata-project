#!/bin/bash

# Script to automatically create Kafka topics.
# Used in the ‘kafka-init’ container.

KAFKA_BROKER="kafka:9092"

# Function to create a topic with name, partitions and replies
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


# create_topic "nome_topic" num partitions num replicas
create_topic "user_annotations" 1 1 
create_topic "europeana_metadata" 1 1


echo " Waiting for Kafka to become ready..."


# Wait for topics to be created
echo "Waiting for topics to be created..."
until /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list >/dev/null 2>&1; do
  sleep 5
done      
# Verify topic creation
echo "Listing all topics..."
/opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server $KAFKA_BROKER \
  --list    


