// Rolle B — Anzeige.
// Live-Verfügbarkeit je Zone aus GET /zones/availability.

function occupancyColor(ratio) {
  if (ratio >= 0.85) return "#dc2626"; // rot: fast voll
  if (ratio >= 0.5) return "#f59e0b";  // gelb
  return "#16a34a";                     // grün: viel frei
}

export default function ZoneGrid({ zones, selected, onSelect }) {
  if (!zones.length) return <p className="muted">Noch keine Daten.</p>;

  return (
    <div className="zone-grid">
      {zones.map((z) => {
        const ratio = z.events_total ? z.occupied / z.events_total : 0;
        return (
          <button
            key={z.zone_id}
            className={`zone-card ${selected === z.zone_id ? "active" : ""}`}
            onClick={() => onSelect(z.zone_id)}
          >
            <div className="zone-head">
              <span className="zone-name">{z.zone_id}</span>
              <span className="dot" style={{ background: occupancyColor(ratio) }} />
            </div>
            <div className="zone-stats">
              <div><strong>{z.free}</strong><span>frei</span></div>
              <div><strong>{z.occupied}</strong><span>belegt</span></div>
            </div>
            <div className="bar">
              <div className="bar-fill" style={{ width: `${ratio * 100}%`, background: occupancyColor(ratio) }} />
            </div>
            <div className="zone-foot">{Math.round(ratio * 100)}% belegt</div>
          </button>
        );
      })}
    </div>
  );
}
