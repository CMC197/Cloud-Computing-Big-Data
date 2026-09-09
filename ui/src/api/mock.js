// Zustandsbehafteter Mock der SmartPark-Pipeline.
//
// Bildet die echte API-Antwortform 1:1 nach (siehe serving/api.py + docs/schema.md):
//   GET  /zones/availability      -> { zones: [...], count }
//   GET  /zones/{zone_id}/history -> { zone_id, points: [...] }   (Route in api.py noch TODO)
//   GET  /zones/{zone_id}/bays    -> { zone_id, bays: [...] }     (Route in api.py noch TODO -> liest Bronze)
//   POST /events                  -> { status, event }
//   GET  /health                  -> { status: "ok" }
//
// POST /events veraendert den internen Zustand -> GET-Antworten aendern sich.
// Damit ist der Rolle-A -> Rolle-B-Durchstich lokal testbar, ganz ohne Cluster.

const NUM_ZONES = 5;
const BAYS_PER_ZONE = 20;

// Zustand je Bucht: occupied bool + Zeitstempel der letzten Aenderung
// (fuer die Bay-Ansicht, die "letztes Event je bay_id" zeigt).
const bays = {};
for (let z = 0; z < NUM_ZONES; z++) {
  const zid = `zone-${z}`;
  bays[zid] = {};
  for (let b = 0; b < BAYS_PER_ZONE; b++) {
    bays[zid][`bay-${z}-${b}`] = {
      occupied: Math.random() < 0.5,
      ts: new Date(Date.now() - Math.random() * 300000).toISOString(),
    };
  }
}

const history = {};
for (let z = 0; z < NUM_ZONES; z++) history[`zone-${z}`] = [];

function aggregate(zid) {
  const states = Object.values(bays[zid]);
  const occupied = states.filter((s) => s.occupied).length;
  const free = states.length - occupied;
  return { occupied, free, events_total: occupied + free };
}

function pushHistoryPoint(zid) {
  const now = new Date();
  const start = new Date(now.getTime() - 60000);
  const agg = aggregate(zid);
  history[zid].push({
    window_start: start.toISOString(),
    window_end: now.toISOString(),
    ...agg,
  });
  if (history[zid].length > 30) history[zid].shift();
}

// Seed rueckwirkende Fenster, damit Charts nicht leer starten
for (let z = 0; z < NUM_ZONES; z++) {
  const zid = `zone-${z}`;
  for (let i = 10; i > 0; i--) {
    const end = new Date(Date.now() - i * 60000);
    const start = new Date(end.getTime() - 60000);
    const occupied = Math.floor(Math.random() * BAYS_PER_ZONE);
    history[zid].push({
      window_start: start.toISOString(),
      window_end: end.toISOString(),
      occupied,
      free: BAYS_PER_ZONE - occupied,
      events_total: BAYS_PER_ZONE,
    });
  }
}

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

export async function mockFetch(path, options = {}) {
  await delay(120);

  if (path === "/health") return { status: "ok" };

  if (path === "/zones/availability") {
    const zones = Object.keys(bays).map((zid) => {
      const agg = aggregate(zid);
      const now = new Date();
      return {
        zone_id: zid,
        window_start: new Date(now.getTime() - 60000).toISOString(),
        window_end: now.toISOString(),
        events_total: agg.events_total,
        occupied: agg.occupied,
        free: agg.free,
      };
    });
    return { zones, count: zones.length };
  }

  const histMatch = path.match(/^\/zones\/([^/]+)\/history$/);
  if (histMatch) {
    const zid = decodeURIComponent(histMatch[1]);
    return { zone_id: zid, points: history[zid] ?? [] };
  }

  const bayMatch = path.match(/^\/zones\/([^/]+)\/bays$/);
  if (bayMatch) {
    const zid = decodeURIComponent(bayMatch[1]);
    const list = bays[zid]
      ? Object.entries(bays[zid]).map(([bay_id, s]) => ({
          bay_id,
          state: s.occupied ? "OCCUPIED" : "FREE",
          event_ts: s.ts,
        }))
      : [];
    list.sort((a, b) => {
      const na = parseInt(a.bay_id.split("-")[2], 10);
      const nb = parseInt(b.bay_id.split("-")[2], 10);
      return na - nb;
    });
    return { zone_id: zid, bays: list };
  }

  if (path === "/events" && options.method === "POST") {
    const body = JSON.parse(options.body);
    const { bay_id, zone_id, state } = body;
    if (bays[zone_id] && bays[zone_id][bay_id]) {
      bays[zone_id][bay_id] = {
        occupied: state === "OCCUPIED",
        ts: new Date().toISOString(),
      };
    }
    pushHistoryPoint(zone_id);
    const now = new Date().toISOString();
    return {
      status: "sent",
      event: { event_id: crypto.randomUUID(), bay_id, zone_id, state, event_ts: now, ingest_ts: now },
    };
  }

  throw new Error(`Mock: unbekannter Pfad ${options.method ?? "GET"} ${path}`);
}
