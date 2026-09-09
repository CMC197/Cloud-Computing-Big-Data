import { useCallback, useEffect, useState } from "react";
import { getAvailability, getHistory, getBays, getHealth } from "./api/client.js";
import { USE_MOCK, API_URL } from "./api/config.js";
import EventInjector from "./components/EventInjector.jsx";
import ZoneGrid from "./components/ZoneGrid.jsx";
import TrendChart from "./components/TrendChart.jsx";
import KpiHeader from "./components/KpiHeader.jsx";
import BayGrid from "./components/BayGrid.jsx";

const POLL_MS = 3000;

export default function App() {
  const [zones, setZones] = useState([]);
  const [selected, setSelected] = useState(null);
  const [history, setHistory] = useState([]);
  const [bays, setBays] = useState([]);
  const [health, setHealth] = useState("?");
  const [error, setError] = useState(null);

  const refreshAvailability = useCallback(async () => {
    try {
      const data = await getAvailability();
      setZones(data.zones);
      setError(null);
      if (!selected && data.zones.length) setSelected(data.zones[0].zone_id);
    } catch (e) {
      setError(e.message);
    }
  }, [selected]);

  const refreshZoneDetail = useCallback(async (zid) => {
    if (!zid) return;
    try {
      const [h, b] = await Promise.all([getHistory(zid), getBays(zid)]);
      setHistory(h.points ?? []);
      setBays(b.bays ?? []);
    } catch {
      // Detail-Routen evtl. noch nicht implementiert (history/bays) -> leer lassen
      setHistory([]);
      setBays([]);
    }
  }, []);

  useEffect(() => {
    getHealth().then((h) => setHealth(h.status)).catch(() => setHealth("down"));
    refreshAvailability();
    const t = setInterval(refreshAvailability, POLL_MS);
    return () => clearInterval(t);
  }, [refreshAvailability]);

  useEffect(() => {
    refreshZoneDetail(selected);
    const t = setInterval(() => refreshZoneDetail(selected), POLL_MS);
    return () => clearInterval(t);
  }, [selected, refreshZoneDetail]);

  const onSent = useCallback(() => {
    refreshAvailability();
    refreshZoneDetail(selected);
  }, [refreshAvailability, refreshZoneDetail, selected]);

  return (
    <div className="app">
      <header>
        <h1>SmartPark</h1>
        <div className="status">
          <span className={`badge ${USE_MOCK ? "mock" : "live"}`}>
            {USE_MOCK ? "MOCK-Modus" : "LIVE"}
          </span>
          <span className="api-url">{USE_MOCK ? "kein Backend noetig" : API_URL}</span>
          <span className={`health health-${health}`}>API: {health}</span>
        </div>
      </header>

      {error && <div className="error-banner">Verbindungsfehler: {error}</div>}

      <KpiHeader zones={zones} />

      <main>
        <div className="col">
          <section className="panel">
            <h2>Rolle B — Live-Verfuegbarkeit</h2>
            <ZoneGrid zones={zones} selected={selected} onSelect={setSelected} />
          </section>
          <EventInjector onSent={onSent} />
        </div>
        <div className="col wide">
          <section className="panel">
            <BayGrid zoneId={selected} bays={bays} />
          </section>
          <section className="panel">
            <TrendChart zoneId={selected} points={history} />
          </section>
        </div>
      </main>
    </div>
  );
}
