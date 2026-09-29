# One image for all five Python processes; docker-compose.prod.yml picks the command.
FROM python:3.11-slim-bookworm

# PySpark 3.5 needs Java (8, 11 or 17).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1
