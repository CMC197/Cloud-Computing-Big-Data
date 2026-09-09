import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev-Server auf 5173. Kein Proxy nötig, solange der Mock-Layer aktiv ist
// (VITE_USE_MOCK=true, Default). Sobald gegen die echte API entwickelt wird,
// VITE_USE_MOCK=false setzen und VITE_API_URL auf die API zeigen lassen —
// oder im Cluster über /config.js (ConfigMap) window.__SMARTPARK_CONFIG__.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,
  },
});
