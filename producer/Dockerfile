# =====================================================================
# AP1 — Event-Simulator / Producer
# Erzeugt synthetische Belegungs-Events und schreibt sie nach Kafka.
# Als Deployment horizontal skalierbar (mehr Replicas = mehr Events/s).
# =====================================================================
FROM python:3.11-slim

# Nicht als root laufen (k8s-Best-Practice, siehe securityContext im Chart)
RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

# Layer-Caching: Dependencies vor dem Quellcode
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER 10001

# Konfiguration kommt aus der ConfigMap (KAFKA_BROKERS, KAFKA_TOPIC, EVENT_RATE, ...)
# Defaults nur als Fallback für lokale Läufe.
ENV KAFKA_BROKERS=smartpark-kafka:9092 \
    KAFKA_TOPIC=parking-events \
    EVENT_RATE=10 \
    PYTHONUNBUFFERED=1

CMD ["python", "simulator.py"]
