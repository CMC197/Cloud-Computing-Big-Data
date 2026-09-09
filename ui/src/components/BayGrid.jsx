// Rolle B — Einzel-Bucht-Ansicht je Zone.
// Zeigt den aktuellen Zustand jeder Parkbucht (letztes Event je bay_id aus Bronze).
// Route GET /zones/{zone_id}/bays ist in serving/api.py noch TODO; der Mock
// liefert sie bereits, die Komponente muss beim Umstieg nicht geaendert werden.

export default function BayGrid({ zoneId, bays }) {
  if (!zoneId) return <p className="muted">Zone anklicken, um die Buchten zu sehen.</p>;
  if (!bays?.length) return <p className="muted">Keine Bucht-Daten fuer {zoneId}.</p>;

  const occupied = bays.filter((b) => b.state === "OCCUPIED").length;
  const free = bays.length - occupied;

  return (
    <div>
      <div className="bay-head">
        <h3>Buchten — {zoneId}</h3>
        <div className="bay-legend">
          <span><i className="sw sw-free" /> {free} frei</span>
          <span><i className="sw sw-occ" /> {occupied} belegt</span>
        </div>
      </div>
      <div className="bay-grid">
        {bays.map((b) => {
          const occ = b.state === "OCCUPIED";
          const num = b.bay_id.split("-").at(-1);
          return (
            <div
              key={b.bay_id}
              className={`bay ${occ ? "bay-occ" : "bay-free"}`}
              title={`${b.bay_id} — ${b.state}${b.event_ts ? " @ " + new Date(b.event_ts).toLocaleTimeString() : ""}`}
            >
              {num}
            </div>
          );
        })}
      </div>
    </div>
  );
}
