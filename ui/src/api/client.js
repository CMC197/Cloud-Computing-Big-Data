// Einheitlicher API-Client. Schaltet zwischen Mock und echter API um
// (gesteuert durch USE_MOCK). Alle Komponenten nutzen NUR diese Funktionen —
// so bleibt der Umstieg auf die echte API ein Ein-Zeilen-Schalter (.env).

import { API_URL, USE_MOCK } from "./config.js";
import { mockFetch } from "./mock.js";

async function call(path, options) {
  if (USE_MOCK) return mockFetch(path, options);

  const res = await fetch(`${API_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${detail || res.statusText}`);
  }
  return res.json();
}

export const getHealth = () => call("/health");

export const getAvailability = () => call("/zones/availability");

export const getHistory = (zoneId) =>
  call(`/zones/${encodeURIComponent(zoneId)}/history`);

export const getBays = (zoneId) =>
  call(`/zones/${encodeURIComponent(zoneId)}/bays`);

export const postEvent = (event) =>
  call("/events", { method: "POST", body: JSON.stringify(event) });
