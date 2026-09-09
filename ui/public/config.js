// Lokaler Platzhalter, damit /config.js im Dev-Modus keinen 404 wirft.
// Im Cluster wird diese Datei per ConfigMap ueberschrieben, z.B.:
//   window.__SMARTPARK_CONFIG__ = { apiUrl: "http://141.72.176.93:30080" };
// Lokal bleibt sie leer -> Fallback in src/api/config.js greift.
window.__SMARTPARK_CONFIG__ = window.__SMARTPARK_CONFIG__ || {};
