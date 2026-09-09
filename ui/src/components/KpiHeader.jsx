// Gesamt-KPIs ueber alle Zonen. Rein aus /zones/availability berechnet —
// keine zusaetzliche API-Route noetig.

export default function KpiHeader({ zones }) {
  const totalFree = zones.reduce((s, z) => s + (z.free ?? 0), 0);
  const totalOcc = zones.reduce((s, z) => s + (z.occupied ?? 0), 0);
  const total = totalFree + totalOcc;
  const occRate = total ? Math.round((totalOcc / total) * 100) : 0;

  // juengstes window_end ueber alle Zonen = "zuletzt aktualisiert"
  const lastUpdate = zones
    .map((z) => z.window_end)
    .filter(Boolean)
    .sort()
    .at(-1);

  const fullest = zones.reduce((best, z) => {
    const r = z.events_total ? z.occupied / z.events_total : 0;
    const br = best && best.events_total ? best.occupied / best.events_total : -1;
    return r > br ? z : best;
  }, null);

  const kpis = [
    { label: "Plaetze frei (gesamt)", value: totalFree, tone: "good" },
    { label: "Belegt (gesamt)", value: totalOcc, tone: "neutral" },
    { label: "Belegungsquote", value: `${occRate}%`, tone: occRate >= 85 ? "bad" : occRate >= 50 ? "warn" : "good" },
    { label: "Aktive Zonen", value: zones.length, tone: "neutral" },
    { label: "Vollste Zone", value: fullest ? fullest.zone_id : "—", tone: "neutral" },
  ];

  return (
    <section className="kpi-header">
      <div className="kpi-row">
        {kpis.map((k) => (
          <div key={k.label} className={`kpi kpi-${k.tone}`}>
            <span className="kpi-value">{k.value}</span>
            <span className="kpi-label">{k.label}</span>
          </div>
        ))}
      </div>
      {lastUpdate && (
        <div className="kpi-updated">
          Zuletzt aktualisiert: {new Date(lastUpdate).toLocaleTimeString()}
        </div>
      )}
    </section>
  );
}
