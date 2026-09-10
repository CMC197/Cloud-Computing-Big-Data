# SmartPark — Streaming-basiertes Smart-Parking-System auf Kubernetes

> **Modul:** Cloud Computing und Big Data · Prüfungsleistung 2026 · Prof. Dr.-Ing. habil. Dennis Pfisterer
> **Gruppe:** `<X>` · **Teammitglieder:** `<Name 1>`, `<Name 2>`, `<Name 3>`, `<Name 4>`
> **Repo:** `<URL>` · **Abgabe:** `<TT.MM.JJJJ>`

<!--
═══════════════════════════════════════════════════════════════════════
  ⚠️  DIESE README IST DAS ALLEINIGE BERICHTSDOKUMENT.
      Ein fehlender Pflichtabschnitt = 0 Punkte im jeweiligen Kriterium.
      Die 12 Überschriften unten NICHT umbenennen oder löschen.
      HTML-Kommentare wie dieser sind Schreibhilfen und werden vor der
      Abgabe entfernt.
═══════════════════════════════════════════════════════════════════════
-->

**Kurzfassung:** SmartPark verarbeitet Belegungsdaten städtischer Parkplatzsensoren
in Echtzeit und stellt Fahrern und Stadtverwaltung die aktuelle Verfügbarkeit je Zone
bereit. Architektur: **Kappa** (streaming-first).
Stack: Kafka → Spark Structured Streaming → Delta Lake auf MinIO → FastAPI → React,
deklarativ deployt per Helm auf einem k3s-Cluster der DHBWCloud.

---

## Inhaltsverzeichnis

1. [Use Case und Motivation](#1-use-case-und-motivation)
2. [Datencharakteristik](#2-datencharakteristik)
3. [Architekturentscheidung: Kappa vs. Lambda](#3-architekturentscheidung-kappa-vs-lambda)
4. [Komponenten und Datenfluss](#4-komponenten-und-datenfluss)
5. [Processing-Logik](#5-processing-logik)
6. [Speicherkonzept](#6-speicherkonzept)
7. [User-facing UI](#7-user-facing-ui)
8. [Kubernetes-Deployment](#8-kubernetes-deployment)
9. [Deployment-Anleitung](#9-deployment-anleitung)
10. [Wesentliche Codeabschnitte](#10-wesentliche-codeabschnitte)
11. [Screenshots und Nachweise](#11-screenshots-und-nachweise)
12. [Grenzen des Prototyps und Ausblick](#12-grenzen-des-prototyps-und-ausblick)

---

## 1. Use Case und Motivation

<!-- 10 P. (zusammen mit §2) · Owner: <Name> · Meilenstein: M6
     Muss enthalten: Problem, Datenquelle, warum das ein Big-Data-Problem ist.
     Argumentationskern: nicht "viele Daten", sondern Velocity + Volume + Variety
     zusammen erzwingen eine Streaming-Lakehouse-Architektur. -->

### 1.1 Problem

`TODO`

### 1.2 Datenquelle

`TODO`

### 1.3 Warum ist das ein Big-Data-Problem?

`TODO`

---

## 2. Datencharakteristik

<!-- Teil der 10 P. aus §1 · Owner: <Name>
     ⚠️ Die V's brauchen KONKRETE ZAHLEN, keine Adjektive.
     Prototyp-Werte UND Zielbild angeben. -->

| V | Ausprägung bei SmartPark | Konkrete Zahlen (Prototyp → Zielbild) |
|---|---|---|
| **Volume** | `TODO` | `TODO` |
| **Velocity** | `TODO` | `TODO` |
| **Variety** | `TODO` | `TODO` |
| *(Veracity)* | `TODO` | `TODO` |

---

## 3. Architekturentscheidung: Kappa vs. Lambda

<!-- 20 P. (zusammen mit §4) · Owner: <Name>
     Pflicht: Entscheidung + Begründung + Warum-nicht-Lambda + Diagramm.
     Das Diagramm MUSS konsistent zu §4 sein — keine verwaisten Boxen. -->

### 3.1 Entscheidung

`TODO`

### 3.2 Begründung

`TODO`

### 3.3 Warum nicht Lambda?

`TODO`

### 3.4 Architekturdiagramm

<!-- Bild nach docs/ legen und hier einbinden: -->
<!-- ![Architekturdiagramm](docs/architektur.png) -->

`TODO`

---

## 4. Komponenten und Datenfluss

<!-- Teil der 20 P. aus §3 · Owner: <Name>
     Jede Technologiewahl BEGRÜNDEN — "haben wir im Kurs gemacht" reicht nicht. -->

### 4.1 Komponenten und Technologiewahl

| Komponente | Technologie | Begründung |
|---|---|---|
| Ingestion | `TODO` | `TODO` |
| Processing | `TODO` | `TODO` |
| Storage | `TODO` | `TODO` |
| Serving-API | `TODO` | `TODO` |
| Query-Layer | `TODO` | `TODO` |
| UI | `TODO` | `TODO` |
| Producer/Simulator | `TODO` | `TODO` |

### 4.2 Ende-zu-Ende-Datenfluss

`TODO`

---

## 5. Processing-Logik

Die gesamte Stream-Verarbeitung ist ein einziger Spark-Structured-Streaming-Job:
[`processing/streaming_job.py`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py).
Eine Kafka-Quelle speist **drei parallele Sinks**, die je eine eigene
Transformation und Ausgabeschicht bedienen. Damit deckt der Job mehr als die
geforderte eine nicht-triviale Transformation ab: eine zeitfenster­basierte
Aggregation, eine zustandsbehaftete Upsert-Logik (Stateful Processing) und die
rohe Persistenz — alle drei aus demselben, per Watermark begrenzten Event-Strom.

### 5.1 Gemeinsame Quelle: Kafka → typisierter Event-Strom mit Watermark

Der Job liest den Kafka-Topic `parking-events`
([Zeilen 82–88](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L82-L88)),
parst den JSON-Payload gegen ein explizites Schema
([Zeilen 25–32](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L25-L32),
[90–95](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L90-L95))
und setzt direkt danach einen **Watermark von 2 Minuten** auf das Feld
`event_ts`
([Zeile 94](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L94)).
Der so typisierte, watermark-behaftete `events`-DataFrame ist die gemeinsame
Grundlage aller drei nachgelagerten Sinks.

### 5.2 Transformation 1 — Windowed Aggregation je Zone (Gold)

Der zentrale analytische Sink aggregiert den Strom in **1-Minuten-Zeitfenstern
je Zone**
([Zeilen 104–119](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L104-L119)):
`groupBy(window(event_ts, "1 minute"), zone_id)` zählt pro Fenster die
Gesamtzahl der Events (`events_total`) sowie — über bedingte Summen
(`sum(when(state = 'OCCUPIED'))` bzw. `'FREE'`) — die belegten und freien
Buchten. Das Ergebnis wird als Delta-Tabelle `gold/zone_availability`
geschrieben
([Zeilen 121–126](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L121-L126)).
Das ist die klassische, nicht-triviale Zeitfenster-Aggregation: aus einem
unendlichen Strom entstehen abgeschlossene, pro Zeitfenster verdichtete
Kennzahlen.

### 5.3 Transformation 2 — Stateful Upsert je Bucht (bay_current)

Für die Live-Ansicht des **aktuellen Zustands jeder einzelnen Parkbucht** genügt
die Gold-Aggregation nicht (dort ist die `bay_id` wegaggregiert). Deshalb pflegt
ein zweiter, zustandsbehafteter Sink eine kompakte Tabelle `gold/bay_current`
mit genau **einer Zeile je Bucht** — dem jeweils jüngsten Zustand.

Die Logik steckt in `upsert_bay_current(batch_df, batch_id)`
([Zeilen 52–74](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L52-L74)),
angebunden über `foreachBatch`
([Zeilen 128–134](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L128-L134)).
Pro Micro-Batch wird per Fensterfunktion
`row_number() OVER (PARTITION BY bay_id ORDER BY event_ts DESC)` das neueste
Event je Bucht bestimmt
([Zeilen 57–62](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L57-L62))
und anschließend per **Delta-`MERGE`** in die Zieltabelle geschrieben
([Zeilen 64–74](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L64-L74)):
vorhandene Buchten werden aktualisiert (nur wenn das neue Event tatsächlich
jünger ist, `condition="s.event_ts > t.event_ts"`), neue Buchten eingefügt
(`whenNotMatchedInsertAll`). Beim allerersten Lauf, wenn die Tabelle noch nicht
existiert, wird sie initial geschrieben
([Zeilen 73–74](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L73-L74)).

Das ist Stateful Processing im eigentlichen Sinn: Der aktuelle Zustand jeder
Bucht wird über Batch-Grenzen hinweg in einer Delta-Tabelle gehalten und
inkrementell fortgeschrieben, statt bei jeder Abfrage neu aus der Rohhistorie
berechnet zu werden. Der Nebeneffekt ist eine winzige, konstant große Tabelle
(≈ Anzahl Buchten), die das Serving ohne teure Scans über die wachsende
Rohschicht bedienen kann.

### 5.4 Transformation 3 — Rohpersistenz (Bronze)

Parallel schreibt ein dritter Sink jedes Event unverändert und append-only in
die Delta-Tabelle `bronze/parking_events`
([Zeilen 97–102](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L97-L102)).
Das ist bewusst keine Transformation, sondern die verlässliche Rohschicht des
Medaillon-Modells: Sie erlaubt, Gold und bay_current bei Bedarf vollständig neu
zu berechnen (Reprocessing).

### 5.5 Windowing, State und Late Data

- **Windowing:** Tumbling Windows von 1 Minute auf `event_ts` (Event-Time, nicht
  Processing-Time), konfigurierbar über `WINDOW_DURATION`
  ([Zeile 17](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L17)).
- **State:** Zwei Formen. Implizit im Windowing (Spark hält die offenen Fenster
  im State-Store); explizit und dauerhaft in der `bay_current`-Delta-Tabelle
  über den Merge.
- **Late Data:** Der Watermark von 2 Minuten
  ([Zeile 18](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L18),
  [94](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L94))
  legt fest, wie lange auf verspätete Events gewartet wird, bevor ein
  Zeitfenster als abgeschlossen gilt und aus dem State entfernt wird. Events,
  die später als 2 Minuten nach ihrem Fenster eintreffen, werden verworfen.
  Damit bleibt der State-Store beschränkt (kein unbegrenztes Wachstum) und die
  Aggregation deterministisch abschließbar.

### 5.6 Warum drei getrennte Sinks statt einem

Bronze (roh), Gold (aggregiert) und bay_current (aktueller Zustand) haben
unterschiedliche Zugriffs- und Aktualisierungsmuster. Die Trennung entkoppelt
sie: Jeder Sink hat einen eigenen `checkpointLocation`
([Zeilen 100](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L100),
[124](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L124),
[132](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L132))
und kann unabhängig wiederanlaufen; `awaitAnyTermination`
([Zeile 136](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py#L136))
hält den Job am Leben, solange mindestens ein Stream läuft.


## 6. Speicherkonzept

<!-- 10 P. · Owner: AP3
     Format, Partitionierung, Schema — jeweils BEGRÜNDET.
     Plus: warum Lakehouse und nicht klassische DB / reiner Data Lake. -->

### 6.1 Schichten (Bronze / Gold)

`TODO`

### 6.2 Format und Begründung

`TODO`

### 6.3 Partitionierung und Begründung

`TODO`

### 6.4 Schema

| Tabelle | Feld | Typ | Beschreibung |
|---|---|---|---|
| `TODO` | | | |

### 6.5 Warum Lakehouse?

`TODO`

---

## 7. User-facing UI

<!-- 10 P. · Owner: AP4
     ⚠️ Die UI muss REAL an die Pipeline angebunden sein (kein Mockup) und als
     eigene containerisierte Komponente auf k8s laufen.
     Beide Rollen beschreiben: Datenlieferant UND Anzeige. -->

### 7.1 Rolle A — Datenlieferant (Event-Injektor)

`TODO`

### 7.2 Rolle B — Anzeige (Verfügbarkeit und Trends)

`TODO`

### 7.3 Anbindung an die Pipeline

`TODO`

### 7.4 Bedienablauf

`TODO`

---

## 8. Kubernetes-Deployment

<!-- 15 P. · Owner: AP5
     Workload-Typen begründen (StatefulSet vs. Deployment), Config über
     ConfigMap/Secret, Persistenz über PVC, und Skalierung ZEIGEN (Screenshots
     in §11 verlinken). -->

### 8.1 Abbildung der Komponenten auf Workloads

| Komponente | Workload-Typ | Begründung | Replicas | Requests / Limits |
|---|---|---|---|---|
| Kafka | `TODO` | | | |
| MinIO | `TODO` | | | |
| Spark | `TODO` | | | |
| Serving-API | `TODO` | | | |
| Producer | `TODO` | | | |
| UI | `TODO` | | | |

### 8.2 Konfiguration (ConfigMaps und Secrets)

`TODO`

### 8.3 Persistenz (PVCs)

`TODO`

### 8.4 Skalierbarkeit

<!-- ⚠️ Rubric-Kernpunkt: "muss darauf ausgelegt sein, in allen Komponenten
     horizontal zu skalieren und dies soll gezeigt werden."
     Nachweis = Screenshots vor/nach Last in §11. -->

`TODO`

---

## 9. Deployment-Anleitung

<!-- 10 P. · Owner: AP5
     DHBWCloud-spezifisch! Voraussetzungen nennen: VPN, VM-Flavor, Netzwerk
     DHBWV6, k3s-Setup, kubeconfig, IPv6-Zugriff.
     Test: Ein anderes Teammitglied muss den Stack allein hochbekommen. -->

### 9.1 Voraussetzungen

`TODO`

### 9.2 Versionen

| Werkzeug | Version |
|---|---|
| `TODO` | |

### 9.3 Schritt für Schritt

`TODO`

### 9.4 Zugriff auf UI und API (IPv6 / Ingress)

`TODO`

---

## 10. Wesentliche Codeabschnitte

Verweise auf die zentralen Stellen im Repository, je mit einer knappen Erklärung,
was dort passiert und warum es so gelöst ist. Die Links zeigen auf einen festen
Commit-Stand, sind also stabil.

### Ingestion — Producer

- [`producer/simulator.py`, Z. 62–68](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/producer/simulator.py#L62-L68)
  — erzeugt fortlaufend synthetische Belegungs-Events und schreibt sie mit
  `key=zone_id` nach Kafka; der Key steuert die Partitionierung, sodass alle
  Events einer Zone garantiert auf derselben Partition und damit in korrekter
  Reihenfolge landen.
- [`producer/simulator.py`, Z. 18 + 52](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/producer/simulator.py#L18)
  — die Sende-Rate ist über die Umgebungsvariable `EVENT_RATE` (Default 10/s)
  steuerbar und bestimmt das Sende-Intervall; sie ist die Stellschraube, an der
  im Skalierungs-Nachweis (§8) gedreht wird.

### Stream Processing — Spark

- [`processing/streaming_job.py`, Z. 82–95](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/processing/streaming_job.py#L82-L95)
  — liest den Kafka-Topic, parst den JSON-Payload gegen ein explizites Schema
  und setzt sofort den Watermark auf `event_ts`; dieser eine `events`-Strom ist
  die gemeinsame Quelle aller drei Sinks (Kappa: ein Verarbeitungspfad).
- [`processing/streaming_job.py`, Z. 104–126](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/processing/streaming_job.py#L104-L126)
  — die Kern-Transformation: aggregiert den Strom in 1-Minuten-Fenstern je Zone
  (Anzahl belegt/frei) und schreibt das Ergebnis als Delta-Tabelle
  `gold/zone_availability`.
- [`processing/streaming_job.py`, Z. 52–74](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/processing/streaming_job.py#L52-L74)
  — Stateful Processing: bestimmt per `foreachBatch` und Fensterfunktion das
  jüngste Event je Bucht und schreibt es per Delta-`MERGE` (Upsert) in
  `gold/bay_current`, sodass dort dauerhaft genau eine aktuelle Zeile pro Bucht
  steht — ohne die Rohhistorie neu scannen zu müssen.
- [`processing/streaming_job.py`, Z. 97–102](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/processing/streaming_job.py#L97-L102)
  — schreibt jedes Event unverändert und append-only in `bronze/parking_events`;
  diese Rohschicht ist die Wahrheit, aus der sich Gold und bay_current jederzeit
  neu berechnen lassen (Reprocessing).

### Serving — API

- [`serving/api.py`, Z. 39–52](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/serving/api.py#L39-L52)
  — öffnet eine DuckDB-Verbindung, die Delta-Tabellen über `delta_scan` aus
  MinIO liest; `delta_scan` liest nur die aktuell gültige Tabellenversion statt
  aller historischen Parquet-Dateien, was Abfragen schnell und speicherschonend
  hält (entscheidend auf der ressourcenknappen VM).
- [`serving/api.py`, Z. 59–83](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/serving/api.py#L59-L83)
  — `GET /zones/availability` liefert die aktuelle Verfügbarkeit je Zone, indem
  es die kompakte `bay_current`-Tabelle nach Zone gruppiert und belegt/frei
  zählt; speist die KPI-Kacheln und die Zonenübersicht der UI.
- [`serving/api.py`, Z. 109–130](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/serving/api.py#L109-L130)
  — `GET /zones/{zone_id}/bays` liefert den aktuellen Zustand jeder einzelnen
  Bucht einer Zone; Datenbasis ist ebenfalls `bay_current`, daher ohne teuren
  Scan der Rohschicht.
- [`serving/api.py`, Z. 133–147](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/serving/api.py#L133-L147)
  — `POST /events` nimmt ein Event der UI entgegen und produziert es mit
  `key=zone_id` nach Kafka; damit speist die UI als Datenlieferant echte Events
  in dieselbe Pipeline wie der Producer und schließt so den Kreis.
- [`serving/api.py`, Z. 31–37](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/serving/api.py#L31-L37)
  — aktiviert CORS, ohne das der Browser die cross-origin-Aufrufe der UI an die
  API blockieren würde (die UI läuft auf einem anderen Port/Origin als die API).

### User-facing UI

- [`ui/src/api/client.js`, Z. 24–33](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/ui/src/api/client.js#L24-L33)
  — kapselt alle Backend-Aufrufe an einer Stelle (`getAvailability`, `getBays`
  für die Anzeige, `postEvent` für den Datenlieferanten); alle Komponenten
  nutzen ausschließlich diese Funktionen, wodurch die Anbindung an die Pipeline
  zentral und austauschbar bleibt.
- [`ui/src/api/config.js`, Z. 15–19](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/ui/src/api/config.js#L15-L19)
  — löst die API-URL zur Laufzeit auf (ConfigMap → ENV → Fallback), sodass die
  Adresse **nicht** ins Image gebacken ist und dasselbe Container-Image ohne
  Neubau in verschiedenen Umgebungen läuft.
- [`ui/src/components/EventInjector.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/ui/src/components/EventInjector.jsx)
  — Rolle A (Datenlieferant): Panel, über das man gezielt oder als Stoß mehrere
  Events erzeugt und per `POST /events` in die Pipeline einspeist.
- [`ui/src/components/ZoneGrid.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/ui/src/components/ZoneGrid.jsx)
  und
  [`BayGrid.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/ui/src/components/BayGrid.jsx)
  — Rolle B (Anzeige): die Zonenübersicht mit Belegungsampel und das
  Bucht-Raster, das den Live-Zustand jeder einzelnen Parkbucht farbcodiert zeigt.

### Manifeste / Deployment

- [`deploy/helm/templates/`](https://github.com/CMC197/Cloud-Computing-Big-Data/tree/f44776b/deploy/helm/templates)
  — ein Helm-Template je Komponente; hier steckt die Abbildung auf
  Workload-Typen: `kafka.yaml` als StatefulSet mit PVC, die übrigen als
  stateless Deployments, und `ui.yaml` inkl. ConfigMap, die `config.js` mit der
  API-URL zur Laufzeit einhängt.
- [`deploy/helm/values.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/deploy/helm/values.yaml)
  — die eine zentrale Konfigurationsdatei für alle Komponenten (Images,
  Replica-Zahlen, Resource-Requests/Limits, Service-Typen, `ui.apiUrl`); trennt
  Konfiguration sauber vom Code.
- [`skaffold.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/f44776b/skaffold.yaml)
  — der reproduzierbare Deploy-Weg: `skaffold run` baut alle vier Images, pusht
  sie nach GHCR und installiert bzw. aktualisiert das Helm-Chart im Cluster in
  einem einzigen Schritt.


## 11. Screenshots und Nachweise

<!-- Ersetzt den Funktionstest — die Anwendung wird bei der Bewertung NICHT
     ausgeführt. Alle Bilder nach docs/screenshots/ und hier einbinden.
     Laufend sammeln, nicht in Woche 7 rekonstruieren! -->

| Nachweis | Status |
|---|---|
| UI im Betrieb (Dashboard + Event-Injektor) | ⬜ |
| Event über die UI einspeisen | ⬜ |
| `kubectl get pods -n smartpark` | ⬜ |
| `kubectl get svc,ingress -n smartpark` | ⬜ |
| `kubectl get hpa` / ScaledObjects | ⬜ |
| Pods **vor** und **nach** Skalierung | ⬜ |
| Serving-Output (`curl` API-Response) | ⬜ |
| Pipeline-Output (Gold-Tabelle / Delta-Query) | ⬜ |
| Kafka-Topic mit Events | ⬜ |

<!-- Einbinden mit:  ![UI im Betrieb](docs/screenshots/ui-dashboard.png) -->

`TODO`

---

## 12. Grenzen des Prototyps und Ausblick

<!-- 5 P. "Reflexion und Eigenanteil" · Owner: Koordination
     Ehrlichkeit zahlt sich hier aus. Die DHBWCloud-Ressourcengrenzen
     offen benennen, z. B. "Spark auf 1 Executor mit 512 MB begrenzt, weil
     der VM-Flavor nur X GB RAM hat; in Produktion würde man ...".
     Plus: Aufgabenverteilung und Eigenanteil (siehe docs/TEAM.md). -->

### 12.1 Scope-Grenzen

`TODO`

### 12.2 Ausblick

`TODO`

### 12.3 Aufgabenverteilung und Eigenanteil

<!-- Aus docs/TEAM.md übernehmen, sobald ausgefüllt. -->

`TODO`

---

## Bonus: Abweichungen vom Standardweg

<!-- Kein Pflichtabschnitt, aber: Bonus gibt es NUR mit Begründung.
     Kandidaten: MinIO/Delta statt HDFS, Exactly-once, Schema-Evolution,
     KEDA auf Consumer-Lag, CI/CD. -->

| Abweichung | Umsetzung | Begründung |
|---|---|---|
| `TODO` | | |

---

## Weiterführende Projektdokumente

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — Branch-Strategie und Commit-Konventionen
- [`docs/TEAM.md`](docs/TEAM.md) — Arbeitspakete, Rollen, Eigenanteil-Log
- [`docs/MEILENSTEINE.md`](docs/MEILENSTEINE.md) — Abgabetermin und Meilensteinplan
