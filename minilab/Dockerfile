FROM python:3.9-slim

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py .

# Default command (can be overridden in docker-compose)
CMD ["python", "03-gen-data.py"]
