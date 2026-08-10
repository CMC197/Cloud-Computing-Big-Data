# =====================================================================
# AP4 — Serving-API (FastAPI)
# Stateless: nimmt Events von der UI an (→ Kafka) und liest die
# Gold-Tabelle zurück. Stateless = horizontal skalierbar per HPA.
# =====================================================================
FROM python:3.11-slim

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER 10001

ENV KAFKA_BROKERS=smartpark-kafka:9092 \
    KAFKA_TOPIC=parking-events \
    S3_ENDPOINT=http://smartpark-minio:9000 \
    DELTA_GOLD_PATH=s3a://smartpark-lakehouse/gold/zone_availability \
    PYTHONUNBUFFERED=1

EXPOSE 8000

# Health-Endpoint /health wird von den Liveness/Readiness-Probes genutzt.
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
