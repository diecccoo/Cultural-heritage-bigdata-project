#!/bin/bash

mkdir -p config/spark/jars
cd config/spark
echo "Scarico i JAR necessari per Spark, Kafka, MinIO, Parquet, Jackson..."

# Spark-Kafka connector
curl -L -o jars/spark-sql-kafka-0-10_2.12-4.0.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/4.0.0/spark-sql-kafka-0-10_2.12-4.0.0.jar

# Kafka client
curl -L -o jars/kafka-clients-3.6.0.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar

# AWS SDK bundle
curl -L -o jars/aws-java-sdk-bundle-1.11.1026.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Spark Kafka token provider (necessario per autenticazione Kafka)
curl -L -o jars/spark-token-provider-kafka-0-10_2.12-4.0.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/4.0.0/spark-token-provider-kafka-0-10_2.12-4.0.0.jar


# # Spark + Kafka connector
# curl -L -o jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
#   https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

# # Spark Kafka token provider (necessario per autenticazione Kafka)
# curl -L -o jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
#   https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# # Kafka client
# curl -L -o jars/kafka-clients-3.5.1.jar \
#   https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

# Hadoop AWS connector per s3a (MinIO)
curl -L -o jars/hadoop-aws.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# curl -L -o jars/aws-java-sdk-bundle.jar \
#   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.426/aws-java-sdk-bundle-1.12.426.jar

# Jackson JSON parsing (usato internamente da Spark + Kafka + MinIO per serializzazione/deserializzazione)
curl -L -o jars/jackson-core-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar

curl -L -o jars/jackson-annotations-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar

curl -L -o jars/jackson-databind-2.14.2.jar \
  https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar

# # Snappy compression (utile per file Parquet)
# curl -L -o jars/snappy-java-1.1.10.1.jar \
#   https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.1/snappy-java-1.1.10.1.jar
#  incluso già nel runtime di Spark; caricarlo due volte può causare conflitti.

# Parquet support
curl -L -o jars/parquet-hadoop-1.12.2.jar \
  https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.12.2/parquet-hadoop-1.12.2.jar

curl -L -o jars/parquet-common-1.12.2.jar \
  https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.12.2/parquet-common-1.12.2.jar

curl -L -o jars/parquet-encoding-1.12.2.jar \
  https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.12.2/parquet-encoding-1.12.2.jar

curl -L -o jars/parquet-column-1.12.2.jar \
  https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.12.2/parquet-column/1.12.2.jar

echo "JAR scaricati nella cartella .config/spark/jars"
