# Stammdaten

Statische Referenzdaten für den Enrichment-Broadcast-Join (AP2/AP3).

| Datei | Inhalt |
|---|---|
| `zones.csv` | zone_id, name, Kapazität, Tarif — TODO(AP3) |
| `bays.csv`  | bay_id, zone_id, Koordinaten — TODO(AP3) |

Diese Dateien **gehören ins Repo** (klein, versionierbar, Teil der Abgabe).
Laufzeitdaten (`bronze/`, `gold/`) dagegen nicht — die sind gitignored.
