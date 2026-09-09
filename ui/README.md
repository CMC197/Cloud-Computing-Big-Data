# SmartPark UI (AP4 — User-facing UI)

React/Vite-Frontend. Baut unabhaengig von der Pipeline: der **Mock-Layer**
(Default an) simuliert die API zustandsbehaftet, sodass der Rolle-A -> Rolle-B-
Durchstich lokal ohne Cluster/VPN testbar ist.

## Lokal starten

```bash
npm install
npm run dev      # http://localhost:5173  (MOCK-Modus)
```

Oben rechts zeigt ein Badge **MOCK-Modus** an. Events im Injektor (Rolle A)
veraendern sofort die Verfuegbarkeit und den Verlauf (Rolle B).

## Gegen die echte API entwickeln

```bash
cp .env.example .env
# in .env:
#   VITE_USE_MOCK=false
#   VITE_API_URL=http://<api-host>:<port>
npm run dev
```

## Rollen

- **Rolle A — Injektor:** `POST /events` (bay_id, zone_id, state) -> Kafka -> Pipeline.
- **Rolle B — Anzeige:** `GET /zones/availability` (Live je Zone),
  `GET /zones/{zone_id}/history` (Trend-Chart).

## API-Contract (aus serving/api.py + docs/schema.md)

| Endpoint | Antwort |
|---|---|
| `GET /zones/availability` | `{ zones: [{zone_id, window_start, window_end, events_total, occupied, free}], count }` |
| `GET /zones/{zone_id}/history` | `{ zone_id, points: [{window_start, window_end, occupied, free, events_total}] }` |
| `GET /zones/{zone_id}/bays` | `{ zone_id, bays: [{bay_id, state, event_ts}] }` — letztes Event je bay_id aus **Bronze** |
| `POST /events` | `{ status, event }` — Body `{bay_id, zone_id, state}` |
| `GET /health` | `{ status: "ok" }` |

> ⚠️ **`/zones/{zone_id}/history` ist in `serving/api.py` noch nicht implementiert.**
> Der Mock liefert die Route bereits. Sobald sie ergaenzt ist (Gold nach zone_id
> filtern, nach window_end sortieren), funktioniert LIVE ohne Aenderung am Frontend.

## Konfiguration im Cluster

Die API-URL wird **nicht** ins Image gebacken. Zur Laufzeit liest das Frontend
`window.__SMARTPARK_CONFIG__.apiUrl` aus `/config.js`, das per ConfigMap gemountet
wird. Fallback-Reihenfolge: ConfigMap -> `VITE_API_URL` -> `http://localhost:8000`.

Beispiel-ConfigMap-Inhalt fuer `config.js`:

```js
window.__SMARTPARK_CONFIG__ = { apiUrl: "http://141.72.176.93:30080" };
```

Dockerfile + nginx.conf (Multi-Stage, Port 8080, non-root) liegen bereits im Repo.

## Build

```bash
npm run build    # -> dist/  (von nginx im Container serviert)
```
