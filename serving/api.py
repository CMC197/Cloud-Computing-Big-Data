"""AP4 — Serving-API (FastAPI).

TODO(AP4), siehe Checkliste Phase 4:
  - GET  /zones/availability      aktuelle Verfuegbarkeit aus der Gold-Tabelle
  - GET  /zones/{zone_id}/history Zeitreihe der Belegung
  - POST /events                  Event von der UI annehmen -> Kafka
  - GET  /health                  fuer Liveness/Readiness-Probes
"""

from fastapi import FastAPI

app = FastAPI(title="SmartPark Serving API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    """Wird von den k8s-Probes aufgerufen."""
    return {"status": "ok"}
