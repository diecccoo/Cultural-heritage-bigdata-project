FROM python:3.10-slim
WORKDIR /app
COPY simulate_user_annotation.py .
RUN pip install kafka-python
CMD ["python", "simulate_user_annotation.py"]
