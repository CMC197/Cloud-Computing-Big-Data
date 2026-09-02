"""AP4 — Serving-API (FastAPI).
Liest die Gold-Tabelle (Parquet auf MinIO) via DuckDB und nimmt Events -> Kafka.
"""
import json
import os
import uuid
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "smartpark-kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "parking-events")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("BUCKET", "smartpark-lakehouse")
GOLD_GLOB = os.getenv("GOLD_GLOB", f"s3://{BUCKET}/gold/zone_availability/*.parquet")

app = FastAPI(title="SmartPark Serving API", version="0.3.0")


def _duck():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_region='us-east-1';")
    return con


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/zones/availability")
def availability():
    try:
        con = _duck()
        rows = con.execute(f"""
            WITH g AS (
                SELECT * FROM read_parquet('{GOLD_GLOB}')
            ),
            latest AS (
                SELECT zone_id, MAX(window_end) AS max_end FROM g GROUP BY zone_id
            )
            SELECT g.zone_id, g.window_start, g.window_end,
                   g.events_total, g.occupied, g.free
            FROM g
            JOIN latest l ON g.zone_id = l.zone_id AND g.window_end = l.max_end
            ORDER BY g.zone_id
        """).fetchall()
        con.close()
        zones = [
            {"zone_id": r[0], "window_start": str(r[1]), "window_end": str(r[2]),
             "events_total": r[3], "occupied": r[4], "free": r[5]}
            for r in rows
        ]
        return {"zones": zones, "count": len(zones)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Gold-Tabelle nicht lesbar: {e}")


class Event(BaseModel):
    bay_id: str
    zone_id: str
    state: str


@app.post("/events")
def post_event(event: Event):
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "event_id": str(uuid.uuid4()),
        "bay_id": event.bay_id,
        "zone_id": event.zone_id,
        "state": event.state,
        "event_ts": now,
        "ingest_ts": now,
    }
    producer.produce(KAFKA_TOPIC, key=event.zone_id, value=json.dumps(payload))
    producer.flush(5)
    return {"status": "sent", "event": payload}
