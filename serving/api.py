"""AP4 — Serving-API (FastAPI).
Liest Delta-Tabellen (MinIO) via DuckDB delta_scan und nimmt Events -> Kafka.
delta_scan liest nur die aktuelle Tabellenversion (schnell, RAM-schonend).
"""
import json
import os
import uuid
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "smartpark-kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "parking-events")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("BUCKET", "smartpark-lakehouse")

GOLD_TABLE = f"s3://{BUCKET}/gold/zone_availability"
BAY_CURRENT_TABLE = f"s3://{BUCKET}/gold/bay_current"

# delta-/httpfs-Extension einmalig beim Start installieren (nicht pro Request)
_boot = duckdb.connect()
_boot.execute("INSTALL delta; INSTALL httpfs;")
_boot.close()

app = FastAPI(title="SmartPark Serving API", version="0.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _duck():
    con = duckdb.connect()
    con.execute("SET memory_limit='400MB';")
    con.execute("LOAD delta;")
    con.execute("LOAD httpfs;")
    endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}',
            ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'
        );
    """)
    return con


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/zones/availability")
def availability():
    """Aktuelle Verfuegbarkeit je Zone, aggregiert aus bay_current (delta_scan)."""
    try:
        con = _duck()
        rows = con.execute(f"""
            SELECT zone_id,
                   COUNT(*) AS events_total,
                   SUM(CASE WHEN state = 'OCCUPIED' THEN 1 ELSE 0 END) AS occupied,
                   SUM(CASE WHEN state = 'FREE' THEN 1 ELSE 0 END) AS free,
                   MAX(event_ts) AS last_ts
            FROM delta_scan('{BAY_CURRENT_TABLE}')
            GROUP BY zone_id
            ORDER BY zone_id
        """).fetchall()
        con.close()
        zones = [
            {"zone_id": r[0],
             "window_start": str(r[4]), "window_end": str(r[4]),
             "events_total": r[1], "occupied": int(r[2]), "free": int(r[3])}
            for r in rows
        ]
        return {"zones": zones, "count": len(zones)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"bay_current nicht lesbar: {e}")


@app.get("/zones/{zone_id}/history")
def zone_history(zone_id: str):
    """Zeitreihe der 1-Minuten-Fenster einer Zone aus der Gold-Tabelle (delta_scan)."""
    try:
        con = _duck()
        rows = con.execute(f"""
            SELECT window_start, window_end, events_total, occupied, free
            FROM delta_scan('{GOLD_TABLE}')
            WHERE zone_id = ?
            ORDER BY window_end DESC
            LIMIT 30
        """, [zone_id]).fetchall()
        con.close()
        points = [
            {"window_start": str(r[0]), "window_end": str(r[1]),
             "events_total": r[2], "occupied": r[3], "free": r[4]}
            for r in reversed(rows)
        ]
        return {"zone_id": zone_id, "points": points}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Gold nicht lesbar: {e}")


@app.get("/zones/{zone_id}/bays")
def zone_bays(zone_id: str):
    """Aktueller Zustand je Bucht einer Zone aus gold/bay_current (delta_scan)."""
    try:
        con = _duck()
        rows = con.execute(f"""
            SELECT bay_id, state, event_ts
            FROM delta_scan('{BAY_CURRENT_TABLE}')
            WHERE zone_id = ?
            ORDER BY bay_id
        """, [zone_id]).fetchall()
        con.close()
        bays = [{"bay_id": r[0], "state": r[1], "event_ts": str(r[2])} for r in rows]
        return {"zone_id": zone_id, "bays": bays}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"bay_current nicht lesbar: {e}")


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
