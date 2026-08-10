# AP4 — User-facing UI

Noch leer. Scaffold anlegen mit:

```bash
cd ui
npm create vite@latest . -- --template react
npm install
```

Danach (siehe Checkliste Phase 5):

- **Rolle A — Datenlieferant:** Event-Injektor-Panel (Bucht belegen/freigeben,
  Simulationsrate). Events gehen per `POST /events` an die API → Kafka → echte Pipeline.
- **Rolle B — Anzeige:** Live-Verfügbarkeitskarte/-tabelle + Trend-Charts.

⚠️ **Die API-URL darf nicht hardcoded sein.** Sie steht erst beim Deploy fest
(Ingress-Host bzw. IPv6-Adresse). Deshalb zur Laufzeit aus `/config.js` lesen,
das per ConfigMap gemountet wird:

```js
const API_URL = window.__SMARTPARK_CONFIG__?.apiUrl ?? "http://localhost:8000";
```

⚠️ Zugriff auf die DHBWCloud funktioniert nur aus dem DHBW-Netz / über VPN.
