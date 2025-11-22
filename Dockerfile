# syntax=docker/dockerfile:1.4
FROM python:3.11-slim

# Install system deps (only what's needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency file
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Cloud Run injects PORT
ENV PORT=8080

# Use shell so $PORT is evaluated
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]
