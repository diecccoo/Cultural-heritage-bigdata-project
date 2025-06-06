FROM python:3.10-slim

WORKDIR /app
COPY mqtt_to_kafka.py .

RUN pip install paho-mqtt kafka-python

CMD ["python", "mqtt_to_kafka.py"]
