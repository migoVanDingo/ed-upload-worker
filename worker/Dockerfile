# Dockerfile
FROM python:3.11-slim

# Install system deps (ffmpeg etc. later if you want)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency file
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Cloud Run will set $PORT
ENV PORT=8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
