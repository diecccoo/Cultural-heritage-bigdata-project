#!/bin/bash

mkdir -p config/spark/jars
cd config/spark/jars

echo "Scarico i JAR necessari per Spark 3.5.0, Kafka, MinIO..."

# Spark + Kafka
curl -L -o spark-sql-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Token provider Kafka
curl -L -o spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Kafka client
curl -L -o kafka-clients-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

# Hadoop AWS
curl -L -o hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK (bundle compatibile)
curl -L -o aws-java-sdk-bundle-1.11.1026.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Jackson JSON (compatibile con Spark + AWS SDK)
curl -L -o jackson-core-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar

curl -L -o jackson-annotations-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar

curl -L -o jackson-databind-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar

echo "✅ Tutti i JAR sono stati scaricati correttamente in config/spark/jars/"
