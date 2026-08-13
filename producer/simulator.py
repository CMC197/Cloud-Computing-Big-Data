"""AP1 — Eent-Simulator.
Erzeugt synthetische Belegungs-Events (FREE <-> OCCUPIED) je Parkbucht und
schreibt sie nach Kafka. Skaliert horizontal über Replicas.
Key = zone_id -> Kafka partitioniert nach Zone.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---- Konfiguration aus ENV (Defaults = lokaler Fallback) ----
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "parking-events")
EVENT_RATE = float(os.getenv("EVENT_RATE", "10"))   # Events pro Sekunde
NUM_ZONES = int(os.getenv("NUM_ZONES", "5"))
BAYS_PER_ZONE = int(os.getenv("BAYS_PER_ZONE", "20"))

# ---- Zustand je Bucht: True = OCCUPIED, False = FREE ----
bays = {
    f"zone-{z}": {f"bay-{z}-{b}": False for b in range(BAYS_PER_ZONE)}
    for z in range(NUM_ZONES)
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(zone_id: str, bay_id: str, occupied: bool) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "bay_id": bay_id,
        "zone_id": zone_id,
        "state": "OCCUPIED" if occupied else "FREE",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main() -> None:
    print(f"Producer startet -> {KAFKA_BROKERS}, topic={KAFKA_TOPIC}, rate={EVENT_RATE}/s")
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    interval = 1.0 / EVENT_RATE if EVENT_RATE > 0 else 1.0

    while True:
        # zufällige Bucht wählen und Zustand umschalten
        zone_id = random.choice(list(bays.keys()))
        bay_id = random.choice(list(bays[zone_id].keys()))
        new_state = not bays[zone_id][bay_id]
        bays[zone_id][bay_id] = new_state

        event = make_event(zone_id, bay_id, new_state)
        producer.produce(
            KAFKA_TOPIC,
            key=zone_id,                       # Partitionierung nach Zone
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval)


if __name__ == "__main__":
    main()
