FROM bitnami/spark:latest

USER root

# installa curl
RUN apt-get update && apt-get install -y curl

# crea cartella per librerie spark
RUN mkdir -p /opt/spark/jars 