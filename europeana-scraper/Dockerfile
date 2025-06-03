# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the local Python scripts and dependencies into the container
COPY europeana_ingest_batch.py .
COPY scheduler.py .
COPY queries.txt .
COPY requirements.txt .

# Install required Python packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run the scheduler script as the main command
CMD ["python", "scheduler.py"]
