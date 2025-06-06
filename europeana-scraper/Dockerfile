# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ensure the state directory exists
RUN mkdir -p /app/state

# Create log file for ingestion
RUN touch europeana_ingestion.log

# Default command: run the scheduler every hour
CMD ["python", "scheduler.py"]
