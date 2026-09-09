// Rolle A — Datenlieferant.
// Sendet Belegungs-Events per POST /events an die API -> Kafka -> Pipeline.

import { useState } from "react";
import { postEvent } from "../api/client.js";

const ZONES = ["zone-0", "zone-1", "zone-2", "zone-3", "zone-4"];
const BAYS_PER_ZONE = 20;

export default function EventInjector({ onSent }) {
  const [zone, setZone] = useState("zone-0");
  const [bay, setBay] = useState("bay-0-0");
  const [state, setState] = useState("OCCUPIED");
  const [busy, setBusy] = useState(false);
  const [log, setLog] = useState([]);

  const bays = Array.from(
    { length: BAYS_PER_ZONE },
    (_, i) => `bay-${zone.split("-")[1]}-${i}`
  );

  async function send(evtState) {
    setBusy(true);
    try {
      const res = await postEvent({ bay_id: bay, zone_id: zone, state: evtState });
      setLog((l) => [
        { ...res.event, ts: new Date().toLocaleTimeString() },
        ...l.slice(0, 7),
      ]);
      onSent?.();
    } catch (e) {
      setLog((l) => [{ error: e.message, ts: new Date().toLocaleTimeString() }, ...l]);
    } finally {
      setBusy(false);
    }
  }

  async function burst() {
    setBusy(true);
    for (let i = 0; i < 10; i++) {
      const z = ZONES[Math.floor(Math.random() * ZONES.length)];
      const b = `bay-${z.split("-")[1]}-${Math.floor(Math.random() * BAYS_PER_ZONE)}`;
      const s = Math.random() < 0.5 ? "OCCUPIED" : "FREE";
      try {
        await postEvent({ bay_id: b, zone_id: z, state: s });
      } catch { /* im Log der Einzel-Sends sichtbar */ }
    }
    setLog((l) => [{ info: "10 zufällige Events gesendet", ts: new Date().toLocaleTimeString() }, ...l.slice(0, 7)]);
    onSent?.();
    setBusy(false);
  }

  return (
    <section className="panel">
      <h2>Rolle A — Event-Injektor</h2>
      <div className="controls">
        <label>
          Zone
          <select value={zone} onChange={(e) => { setZone(e.target.value); setBay(`bay-${e.target.value.split("-")[1]}-0`); }}>
            {ZONES.map((z) => <option key={z} value={z}>{z}</option>)}
          </select>
        </label>
        <label>
          Bucht
          <select value={bay} onChange={(e) => setBay(e.target.value)}>
            {bays.map((b) => <option key={b} value={b}>{b}</option>)}
          </select>
        </label>
        <label>
          Zustand
          <select value={state} onChange={(e) => setState(e.target.value)}>
            <option value="OCCUPIED">OCCUPIED</option>
            <option value="FREE">FREE</option>
          </select>
        </label>
      </div>
      <div className="btn-row">
        <button disabled={busy} onClick={() => send(state)}>Event senden</button>
        <button disabled={busy} className="secondary" onClick={burst}>10 zufällige Events</button>
      </div>
      <div className="log">
        {log.map((entry, i) => (
          <div key={i} className={entry.error ? "log-err" : "log-ok"}>
            <span className="log-ts">{entry.ts}</span>{" "}
            {entry.error
              ? `Fehler: ${entry.error}`
              : entry.info
              ? entry.info
              : `${entry.zone_id} / ${entry.bay_id} -> ${entry.state}`}
          </div>
        ))}
      </div>
    </section>
  );
}
