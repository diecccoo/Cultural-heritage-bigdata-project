FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy all source files
COPY . .

# Ensure state directory exists in container (even if host has no files yet)
RUN mkdir -p /app/state

# Create empty log file
RUN touch /app/europeana_ingestion.log

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the scheduler
CMD ["python", "scheduler.py"]
