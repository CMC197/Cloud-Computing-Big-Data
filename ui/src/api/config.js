// Zentrale Auflösung der API-URL und des Mock-Schalters.
//
// Priorität der API-URL:
//   1. window.__SMARTPARK_CONFIG__.apiUrl   (Cluster: ConfigMap -> /config.js)
//   2. import.meta.env.VITE_API_URL         (lokaler .env-Override)
//   3. "http://localhost:8000"              (Fallback)
//
// Mock-Layer aktiv, wenn VITE_USE_MOCK !== "false". Default: an.
// So entwickelst du komplett ohne Cluster/VPN. Zum Andocken an die echte
// API: .env mit VITE_USE_MOCK=false und ggf. VITE_API_URL anlegen.

const runtime =
  typeof window !== "undefined" ? window.__SMARTPARK_CONFIG__ : undefined;

export const API_URL =
  runtime?.apiUrl ??
  import.meta.env.VITE_API_URL ??
  "http://localhost:8000";

export const USE_MOCK = import.meta.env.VITE_USE_MOCK !== "false";
