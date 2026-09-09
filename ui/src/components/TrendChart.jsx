// Rolle B — Trend-Chart je Zone aus GET /zones/{zone_id}/history.
// Hinweis: diese Route ist in serving/api.py noch TODO. Der Mock liefert sie
// bereits; sobald der Kollege sie ergaenzt, funktioniert der echte Modus ohne
// Aenderung an dieser Komponente.

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";

export default function TrendChart({ zoneId, points }) {
  if (!zoneId) return <p className="muted">Zone anklicken, um den Verlauf zu sehen.</p>;
  if (!points?.length) return <p className="muted">Kein Verlauf fuer {zoneId}.</p>;

  const data = points.map((p) => ({
    time: new Date(p.window_end).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
    belegt: p.occupied,
    frei: p.free,
  }));

  return (
    <div>
      <h3>Verlauf — {zoneId}</h3>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: -8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="time" fontSize={12} />
          <YAxis fontSize={12} />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="belegt" stroke="#dc2626" strokeWidth={2} dot={false} />
          <Line type="monotone" dataKey="frei" stroke="#16a34a" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
