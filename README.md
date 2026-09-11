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

### 3.1 Entscheidung

SmartPark ist als **Kappa-Architektur** umgesetzt: Es gibt genau einen
Verarbeitungspfad — Kafka als alleinige Quelle der Wahrheit, ein einziger
Spark-Structured-Streaming-Job, der kontinuierlich läuft, und drei
Delta-Tabellen als materialisierte Ausgaben desselben Stroms. Es existiert
kein separater Batch-Layer, keine zweite Code-Basis für historische
Neuberechnungen und keine periodisch angestoßene Voll-Neuberechnung der
Historie außerhalb des Streams. Jede Komponente der Pipeline — vom Producer
über Kafka bis zur Serving-API — geht implizit von genau einem
Verarbeitungspfad aus; das ist keine nachträgliche Beobachtung, sondern die
Grundannahme, auf der der gesamte Systementwurf beruht.

### 3.2 Begründung

**Ein Datenpfad statt zwei Sichten mit unterschiedlicher Genauigkeit.** Lambda
existiert historisch, um einen Kompromiss zu lösen: Ein Speed-Layer liefert
schnelle, aber potenziell ungenaue Ergebnisse; ein Batch-Layer korrigiert diese
später mit der vollständigen, konsolidierten Sicht. SmartPark hat diesen
Kompromiss nicht — es gibt keinen fachlichen Grund, warum die
Parkplatz-Verfügbarkeit "grob jetzt" und "exakt morgen" unterschiedlich
aussehen müsste. Eine belegte Bucht ist belegt, sobald das Event verarbeitet
ist; es gibt keine nachträgliche Korrektur, die fachlich sinnvoll wäre. Damit
entfällt der eigentliche Daseinszweck von Lambda für diesen Use Case.

**Late Data ist bereits im Streaming-Pfad gelöst, nicht nachträglich im
Batch.** Der klassische Grund, überhaupt einen Batch-Layer zu betreiben, ist,
verspätete oder nachträglich korrigierte Daten in die "endgültige" Sicht
einzurechnen. Bei SmartPark übernimmt das der Watermark direkt im Stream
(2 Minuten auf `event_ts`, §5.5): Events, die innerhalb dieses Fensters
eintreffen, werden korrekt eingerechnet, bevor ein Zeitfenster als
abgeschlossen gilt; spätere Events werden bewusst verworfen. Ein zusätzlicher
Batch-Layer würde exakt dieselbe Aufgabe — verspätete Daten korrekt behandeln
— ein zweites Mal und mit anderer Semantik lösen, ohne einen erkennbaren
fachlichen Mehrwert zu bringen.

**Eine Code-Basis statt zwei parallel gepflegter Implementierungen.** Bei
Lambda müsste die Aggregationslogik (hier: die Windowed Aggregation aus §5.2)
zweimal implementiert werden — einmal für den Speed-Layer (Streaming) und
einmal für den Batch-Layer (z. B. ein täglicher Spark-Batch-Job über die volle
Historie) — und beide Implementierungen müssten bei jeder Änderung
synchron gehalten werden. Das ist eine notorische Fehlerquelle in
Lambda-Systemen ("das Batch-Ergebnis weicht vom Speed-Ergebnis ab, weil beim
letzten Update nur eine Seite angepasst wurde"). Bei Kappa steckt die gesamte
Verarbeitungslogik exakt einmal in
[`streaming_job.py`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/1f0ce5c/processing/streaming_job.py)
— es gibt nichts, was auseinanderlaufen könnte.

**Reprocessing ersetzt den Batch-Layer, statt ihn zu ergänzen.** Der andere
klassische Nutzen eines Batch-Layers — die Möglichkeit, die komplette Historie
bei Bedarf neu zu berechnen, etwa nach einem Bugfix in der Aggregationslogik —
ist bei Kappa durch die Bronze-Schicht (§6.1) abgedeckt: Da jedes Rohevent
unverändert und vollständig in `bronze/parking_events` liegt, lässt sich Gold
oder bay_current jederzeit aus Bronze neu berechnen, indem der Streaming-Job
erneut über die (oder Teile der) Bronze-Historie läuft. Das ist genau das
Kappa-Versprechen: Reprocessing durch erneutes Abspielen des Stroms, statt
durch eine zweite, dauerhaft mitlaufende Infrastruktur.

**Ressourcen-Passung auf der Ziel-Hardware.** Auf der DHBWCloud-VM mit 12 GB
RAM (§12.1) wäre ein zusätzlicher Batch-Layer — typischerweise ein eigener,
periodisch angestoßener Spark-Job mit eigenem Scheduling und eigenem
Ressourcenbedarf — ein weiterer, schwer zu rechtfertigender RAM- und
CPU-Verbraucher neben dem bereits knapp bemessenen Streaming-Job (`local[2]`,
siehe §12.1 zu den dort real aufgetretenen `OOMKilled`-Ereignissen). Kappa
passt damit nicht nur fachlich, sondern auch infrastrukturell besser zu den
tatsächlich verfügbaren Ressourcen dieses Prototyps.

### 3.3 Warum nicht Lambda?

Lambda wäre die naheliegende Alternative gewesen, wenn SmartPark zwei fachlich
unterschiedliche Antworten auf dieselbe Frage bräuchte — etwa eine schnelle,
aber ungenaue Live-Schätzung der Verfügbarkeit für die App-Anzeige *und*
parallel eine exakte, aber verzögerte End-of-Day-Statistik für Abrechnungs-
oder Auslastungsberichte an die Stadtverwaltung. Ein solches Szenario ist
denkbar, liegt aber außerhalb des hier gewählten Scopes (siehe Ausblick,
§12.2, zu möglichen künftigen Auswertungen). Im aktuellen Scope sind sowohl
die Zeitfenster-Aggregation (Gold, §5.2) als auch der aktuelle Bucht-Zustand
(bay_current, §5.3) *dieselbe Art* von Antwort auf denselben Datenstrom, nur
unterschiedlich geformt für unterschiedliche Lesezugriffe — nicht zwei
Sichten unterschiedlicher Genauigkeit oder Aktualität. Es gibt also keinen
fachlichen Bedarf, dieselbe zugrunde liegende Frage zweimal (einmal im
Batch-, einmal im Speed-Layer) zu beantworten. Der Mehraufwand von Lambda —
zwei getrennte Pipelines, ein Mechanismus zum Abgleich/Merge von Batch- und
Speed-Ergebnissen, doppelte Infrastruktur und doppelter Betriebsaufwand —
stünde in keinem Verhältnis zum Nutzen für diesen Use Case und wäre auf der
verfügbaren Hardware zusätzlich riskant gewesen, wie die im Betrieb real
aufgetretenen Ressourcenengpässe (§12.1) zeigen.

### 3.4 Architekturdiagramm

![SmartPark Kappa-Architektur — Ende-zu-Ende-Datenfluss](docs/architektur.png)

**Lesart:** Der durchgezogene Pfad ist der reguläre Datenfluss (Producer → Kafka → Spark → drei Delta-Tabellen → API → UI-Anzeige). Der gestrichelte Pfad zeigt den geschlossenen Kreis: Die UI kann als Datenlieferant selbst Events erzeugen (`POST /events` → API → Kafka), die in denselben Verarbeitungspfad einspeisen — kein zweiter, paralleler Weg (Kappa, §3.1).


## 4. Komponenten und Datenfluss

### 4.1 Komponenten und Technologiewahl

| Komponente | Technologie | Begründung |
|---|---|---|
| **Ingestion** | Apache Kafka (KRaft-Modus, ohne ZooKeeper) | Standard für ein Streaming-Backbone; entkoppelt Producer und Verarbeitung (Puffer bei Lastspitzen) und liefert mit Partitionen die natürliche Parallelitäts-Achse (§8.4). KRaft spart den separaten ZooKeeper-Prozess — auf der RAM-knappen VM (§12.1) ein relevanter Vorteil. |
| **Processing** | Apache Spark Structured Streaming (PySpark) | Passt zum Lehrstoff und deckt alle geforderten Streaming-Konzepte nativ ab: Event-Time-Windowing, Watermarks/Late-Data und über `foreachBatch` auch Stateful Upserts (§5). Ein einziger Job bedient drei Sinks — konsistent mit Kappa. |
| **Storage** | Delta Lake auf MinIO (S3-kompatibel) | Lakehouse statt reiner DB oder nacktem Data Lake: ACID-Writes für mehrere parallele Sinks, Upserts (`MERGE`) für `bay_current`, effizientes Lesen der aktuellen Version via `delta_scan` (§6.2). MinIO entkoppelt Storage von Compute und ist leichter als HDFS (Bonus-Abweichung, §12). |
| **Serving-API** | FastAPI (Python) | Leichtgewichtiges, asynchrones Web-Framework; stellt die drei Read-Endpunkte und `POST /events` bereit und lässt sich als zustandslose Komponente mit mehreren Replicas betreiben (§8.1). |
| **Query-Layer** | DuckDB mit `delta_scan()` | Liest die Delta-Tabellen direkt aus MinIO, ohne einen zweiten dauerhaften Spark-Prozess für Lesezugriffe — auf der 12-GB-VM eine bewusst RAM-schonende Wahl (Bonus-Abweichung, §12). `delta_scan` liest nur die aktuelle Tabellenversion (§6.2). |
| **UI** | React (Vite), als Nginx-Container | Eigenständige, containerisierte Frontend-Komponente auf k8s (kein Mockup). Deckt beide geforderten Rollen ab: Datenlieferant (Event-Injektor) und Anzeige (§7). API-URL kommt zur Laufzeit aus einer ConfigMap, nicht ins Image gebacken. |
| **Producer/Simulator** | Python + `confluent-kafka` | Erzeugt den synthetischen Event-Strom (mangels echter Parksensoren). Sende-Rate über `EVENT_RATE` steuerbar — die Stellschraube des Skalierungs-Nachweises (§8.4). Partitioniert nach `zone_id`. |

Ergänzend: **k3s** als leichtgewichtige Kubernetes-Distribution, **Helm** als deklaratives Deployment-Paket und **Skaffold** als Build-/Deploy-Werkzeug (`skaffold run` baut alle Images, pusht nach GHCR und installiert das Chart — §8, §9).

### 4.2 Ende-zu-Ende-Datenfluss

Der Datenfluss ist ein einziger, geschlossener Kreis (siehe Diagramm §3.4):

1. **Erzeugung.** Der **Producer** erzeugt fortlaufend Belegungs-Events
   (`bay_id`, `zone_id`, `state`, Zeitstempel) und schreibt sie mit
   `key=zone_id` nach **Kafka** (Topic `parking-events`). Alternativ erzeugt
   die **UI (Rolle A)** einzelne Events über `POST /events`, die die API
   ebenfalls nach Kafka produziert — dieselbe Quelle, kein Sonderweg.
2. **Verarbeitung.** Der **Spark**-Streaming-Job konsumiert den Topic, parst
   die Events gegen ein festes Schema und setzt einen Watermark (2 min). Aus
   diesem einen Strom entstehen parallel drei Delta-Tabellen:
   **Bronze** (jedes Event roh, append-only), **Gold** (1-Minuten-Aggregate je
   Zone) und **bay_current** (aktueller Zustand je Bucht, via Delta-Merge).
   Die drei Tabellen werden unabhängig aus dem Strom gebildet — Gold liest
   *nicht* aus Bronze (§5).
3. **Speicherung.** Alle drei Tabellen liegen als Delta auf **MinIO** im Bucket
   `smartpark-lakehouse` (§6).
4. **Bereitstellung.** Die **FastAPI**-Serving-Schicht liest über **DuckDB /
   `delta_scan`** aus Gold und bay_current und stellt sie als JSON-Endpunkte
   bereit (`GET /zones/availability`, `GET /zones/{zone_id}/bays`).
5. **Anzeige.** Die **UI (Rolle B)** pollt diese Endpunkte und zeigt
   Zonen-Verfügbarkeit (KPIs) und den Zustand jeder einzelnen Bucht an.
6. **Kreisschluss.** Ein über die UI (Rolle A) eingespeistes Event durchläuft
   die komplette Kette (UI → API → Kafka → Spark → Delta → API → UI) und wird
   in der Anzeige sichtbar — der Nachweis, dass die UI real an die Pipeline
   angebunden ist (kein Mockup).


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

### 6.1 Schichten (Medaillon-Architektur)

Das Lakehouse liegt auf MinIO im Bucket `smartpark-lakehouse` und gliedert
sich in drei Delta-Tabellen mit unterschiedlichem Zweck (Medaillon-Muster,
innerhalb des einen Kappa-Streaming-Pfads — siehe §5.6):

| Schicht | Pfad | Inhalt | Aktualisierung |
|---|---|---|---|
| **Bronze** | `bronze/parking_events` | jedes Rohevent unverändert, append-only | fortlaufend, ein Schreibvorgang je Micro-Batch |
| **Gold** | `gold/zone_availability` | 1-Minuten-Aggregate je Zone (belegt/frei) | fortlaufend, ein neues Fenster je Minute |
| **bay_current** | `gold/bay_current` | genau eine Zeile je Bucht: deren aktuellster Zustand | fortlaufend per Delta-Merge (Upsert), Zeilenzahl bleibt konstant |

Bronze ist die vollständige, unveränderte Wahrheit — daraus lassen sich Gold
und bay_current jederzeit neu berechnen (Reprocessing). Gold und bay_current
sind zwei unterschiedlich geformte, abfrageoptimierte Sichten auf denselben
Strom: Gold beantwortet „wie war die Belegung je Zeitfenster", bay_current
beantwortet „wie ist der Zustand *jetzt*, je Bucht" (siehe §5.3).

### 6.2 Format und Begründung: Delta statt reines Parquet

Alle drei Tabellen liegen im **Delta-Format** (Parquet-Dateien plus
Transaktionslog `_delta_log`), nicht als nacktes Parquet. Gründe:

- **ACID-Schreibvorgänge:** Mehrere Micro-Batches schreiben fortlaufend in
  dieselbe Tabelle; Delta garantiert, dass ein lesender Client nie einen
  halbgeschriebenen Zustand sieht.
- **Schema-Enforcement:** Verhindert, dass ein fehlerhafter Batch die Tabelle
  mit einem falschen Schema verunreinigt.
- **Upsert-Fähigkeit (`MERGE`):** wird von `bay_current` zwingend benötigt
  (§5.3) — mit reinem Parquet wäre ein Update einzelner Zeilen nicht
  möglich, ohne die ganze Tabelle neu zu schreiben.
- **Effizientes Lesen der aktuellen Version (`delta_scan`):** Das
  Transaktionslog erlaubt der Serving-API, über DuckDBs `delta_scan()` gezielt
  nur die aktuell gültigen Dateien zu lesen, statt bei jeder Anfrage alle
  historischen Parquet-Dateien zu öffnen. Das war im Betrieb kein
  theoretischer Vorteil, sondern eine reale Notwendigkeit: Ein früherer Ansatz
  über `read_parquet('*.parquet')` musste bei wachsender Dateizahl *alle*
  jemals geschriebenen Dateiversionen öffnen und wurde nach einigen Stunden
  Laufzeit so langsam, dass Anfragen in Timeouts liefen (siehe §12). Der
  Wechsel auf `delta_scan` löste das strukturell, weil nur die aktuelle
  Tabellenversion gelesen wird.

### 6.3 Partitionierung und Begründung

- **Bronze** ist zeitlich implizit partitioniert durch die fortlaufenden
  Micro-Batch-Schreibvorgänge; eine explizite Partitionierungsspalte ist bei
  append-only-Rohdaten nicht nötig, da nie gezielt nach Zone gefiltert
  geschrieben oder gelesen wird.
- **Gold** ist inhaltlich nach `zone_id` und Zeitfenster (`window_start`)
  strukturiert — beides Ergebnis des `groupBy(window(...), zone_id)` in der
  Aggregation (§5.2). Das entspricht dem typischen Zugriffsmuster: „Verlauf
  einer Zone über die Zeit".
- **bay_current** ist nach `bay_id` dedupliziert (eine Zeile pro Bucht) und
  wird von der API zusätzlich nach `zone_id` gefiltert — die Tabelle ist mit
  wenigen hundert Zeilen so klein, dass eine physische Partitionierung keinen
  Mehrwert brächte; ihr eigentlicher Effizienzgewinn liegt in der konstanten
  Größe (siehe 6.1), nicht in der Partitionierung.
- Auf Kafka-Ebene (vorgelagert) partitioniert der Producer nach `zone_id`
  (§10) — diese Partitionierung bestimmt indirekt auch die Reihenfolge, in
  der Events bei Spark ankommen, und damit die Konsistenz der Fenster.

### 6.4 Schema

**Bronze — `bronze/parking_events`**

| Feld | Typ | Beschreibung |
|---|---|---|
| `event_id` | String | eindeutige Event-ID (UUID) |
| `bay_id` | String | ID der Parkbucht, z. B. `bay-2-7` |
| `zone_id` | String | ID der Zone, z. B. `zone-2` — auch Kafka-Partitionierungs-Key |
| `state` | String | `FREE` oder `OCCUPIED` |
| `event_ts` | Timestamp | Zeitpunkt des Ereignisses (Event-Time, Basis für Watermark) |
| `ingest_ts` | Timestamp | Zeitpunkt der Erzeugung im Producer |

**Gold — `gold/zone_availability`**

| Feld | Typ | Beschreibung |
|---|---|---|
| `window_start` | Timestamp | Beginn des 1-Minuten-Fensters |
| `window_end` | Timestamp | Ende des Fensters |
| `zone_id` | String | ID der Zone |
| `events_total` | Long | Anzahl Events im Fenster |
| `occupied` | Long | Anzahl `OCCUPIED`-Events im Fenster |
| `free` | Long | Anzahl `FREE`-Events im Fenster |

**bay_current — `gold/bay_current`**

| Feld | Typ | Beschreibung |
|---|---|---|
| `bay_id` | String | ID der Parkbucht (Merge-Schlüssel) |
| `zone_id` | String | ID der Zone |
| `state` | String | aktuellster bekannter Zustand (`FREE`/`OCCUPIED`) |
| `event_ts` | Timestamp | Zeitstempel des zugrundeliegenden, jüngsten Events |

### 6.5 Warum Lakehouse statt klassischer Datenbank oder reinem Data Lake?

Ein reiner Data Lake (nacktes Parquet/CSV ohne Transaktionslog) hätte keine
sicheren nebenläufigen Schreibvorgänge und keine Upsert-Fähigkeit geboten —
beides wird von einem Streaming-Job mit drei parallelen Sinks zwingend
gebraucht (§5). Eine klassische relationale Datenbank hätte umgekehrt die
Rohdaten-Vollständigkeit (Bronze) und die güns­tige Objektspeicherung großer,
unstrukturierter Ereignismengen erschwert und wäre für den Betrieb auf einer
ressourcenbegrenzten VM (kein separat betriebener DB-Server) unpassender
gewesen. Das Lakehouse-Modell (Delta auf MinIO) verbindet die Skalierbarkeit
und geringen Betriebskosten eines Objektspeichers mit den
Konsistenzgarantien einer Datenbank — genau der Mittelweg, den Bronze/Gold/
bay_current in diesem Projekt brauchen. MinIO als S3-kompatibler
Objektspeicher entkoppelt zusätzlich Storage von Compute: Der Spark-Job kann
neu gestartet oder skaliert werden, ohne dass die Daten davon betroffen sind
— eine bewusste, im Bericht begründete Abweichung von HDFS (siehe Bonus,
§12).


## 7. User-facing UI

Die UI ist eine eigenständige React-Anwendung (Vite-Build, als Nginx-Container
auf Kubernetes deployt — §8.1), die **real** an die laufende Pipeline
angebunden ist (kein Mockup): Sämtliche Anzeigen stammen aus der Serving-API,
und eingespeiste Events durchlaufen dieselbe Pipeline wie die des Producers.
Sie deckt beide geforderten Rollen ab.

### 7.1 Rolle A — Datenlieferant (Event-Injektor)

Der Event-Injektor
([`EventInjector.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/components/EventInjector.jsx))
erlaubt es, gezielt einzelne Belegungs-Events in die Pipeline einzuspeisen: Über
Auswahlfelder wählt man **Zone** (`zone-0` … `zone-4`), **Bucht** (`bay-<z>-<n>`)
und **Zustand** (`OCCUPIED` / `FREE`) und löst mit „Event senden" einen
`POST /events` an die API aus. Zusätzlich erzeugt ein Button „10 zufällige
Events" einen kleinen Stoß zufälliger Events (Burst) — praktisch, um schnell
Last zu erzeugen und die Reaktion der Anzeige zu beobachten. Jedes gesendete
Event wird in einem kleinen Log-Bereich mitprotokolliert. Das ist die
konkrete Umsetzung der Datenlieferanten-Rolle: Der Mensch erzeugt über die
Weboberfläche echte Events, die in die Ingestion fließen.

### 7.2 Rolle B — Anzeige (Live-Verfügbarkeit)

Die Anzeige besteht aus drei aufeinander abgestimmten Bausteinen:

- **KPI-Kopfzeile**
  ([`KpiHeader.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/components/KpiHeader.jsx))
  — aggregierte Kennzahlen über alle Zonen: freie/belegte Plätze gesamt,
  Belegungsquote und Anzahl aktiver Zonen.
- **Zonen-Übersicht**
  ([`ZoneGrid.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/components/ZoneGrid.jsx))
  — je Zone eine Karte mit belegt/frei-Zahlen und einer farbigen
  Belegungs-Ampel; Datenquelle ist `GET /zones/availability`.
- **Bucht-Raster**
  ([`BayGrid.jsx`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/components/BayGrid.jsx))
  — nach Auswahl einer Zone der Zustand jeder einzelnen Parkbucht als
  farbcodiertes Raster (grün = frei, rot = belegt); Datenquelle ist
  `GET /zones/{zone_id}/bays`.

Die Anzeige aktualisiert sich per Polling in kurzen Intervallen, sodass
eingespeiste Events nach dem Pipeline-Durchlauf automatisch sichtbar werden.

> Hinweis: Eine ursprünglich geplante Trend-/Verlaufsansicht je Zone (aus der
> Gold-Tabelle) wurde bewusst entfernt, da die zugrunde liegende Abfrage auf
> der ressourcenbegrenzten VM zu teuer wurde (siehe §12.1). Die
> Live-Verfügbarkeit (Zonen und Buchten) ist davon nicht betroffen.

### 7.3 Anbindung an die Pipeline

Alle Backend-Aufrufe laufen zentral über einen API-Client
([`ui/src/api/client.js`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/api/client.js)):
`getAvailability` und `getBays` für die Anzeige, `postEvent` für den
Datenlieferanten. Die **API-Basis-URL wird zur Laufzeit** aufgelöst
([`config.js`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/e9ee9ad/ui/src/api/config.js)) —
über `window.__SMARTPARK_CONFIG__`, das im Cluster aus einer ConfigMap als
`config.js` in den Nginx-Container gemountet wird (§8.2). Die URL ist damit
**nicht ins Image gebacken**; dasselbe Container-Image läuft ohne Neubau in
verschiedenen Umgebungen. Damit ist die UI eine vollwertige, real angebundene
Komponente und kein Mockup.

### 7.4 Bedienablauf (Durchstich)

Der typische Ablauf, der zugleich den Ende-zu-Ende-Durchstich zeigt:

1. Nutzer wählt im Injektor (Rolle A) Zone, Bucht und Zustand und klickt „Event
   senden".
2. Die UI sendet `POST /events` → die API produziert das Event nach Kafka.
3. Der Spark-Job verarbeitet es und aktualisiert Gold und bay_current.
4. Die Anzeige (Rolle B) pollt die API und zeigt die geänderte Verfügbarkeit
   bzw. die umgefärbte Bucht-Kachel an.

Dieser sichtbare Kreislauf — Event über die UI rein, verarbeitet wieder
heraus — ist der Nachweis der realen Pipeline-Anbindung.


## 8. Kubernetes-Deployment

Alle sechs Komponenten sind über ein einziges Helm-Chart
([`deploy/helm/`](https://github.com/CMC197/Cloud-Computing-Big-Data/tree/62a3e86/deploy/helm))
deklarativ auf k3s deployt. Konfiguration lebt zentral in
[`values.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/values.yaml),
sodass jede Komponente einzeln (de-)aktiviert, skaliert und mit
Ressourcen-Grenzen versehen werden kann, ohne Templates anzufassen.

### 8.1 Abbildung auf Workload-Typen

| Komponente | Workload | Begründung |
|---|---|---|
| Kafka | **StatefulSet** ([`kafka.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/kafka.yaml)) | braucht eine stabile Netzwerk-Identität und ein eigenes, dauerhaftes Volume je Broker — genau das liefert ein StatefulSet, ein Deployment nicht. |
| MinIO | StatefulSet-artig betrieben, PVC-gebunden | hält die Lakehouse-Daten; ebenfalls zustandsbehaftet. |
| Producer | **Deployment** ([`producer.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/producer.yaml)) | zustandslos, beliebig viele austauschbare Replicas — Deployment ist der richtige Typ. |
| Spark-Processing | **Deployment** ([`processing.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/processing.yaml)) | ein laufender Streaming-Job; State liegt nicht im Pod, sondern in den Delta-Checkpoints/-Tabellen auf MinIO. |
| Serving-API | **Deployment**, 2 Replicas ([`serving.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/serving.yaml)) | zustandslos, liest bei jeder Anfrage frisch aus MinIO — mehrere Replicas ohne Koordinationsaufwand möglich. |
| UI | **Deployment** ([`ui.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/ui.yaml)) | statisches Nginx-Frontend, zustandslos. |

### 8.2 Konfiguration und Persistenz

- **ConfigMaps:** Die UI erhält ihre API-Adresse zur Laufzeit über eine
  ConfigMap, die `config.js` nach `/usr/share/nginx/html/` mountet
  ([`ui.yaml`](https://github.com/CMC197/Cloud-Computing-Big-Data/blob/62a3e86/deploy/helm/templates/ui.yaml)) —
  die URL ist damit nicht ins Container-Image gebacken.
- **Secrets:** Die MinIO-Zugangsdaten (Access-/Secret-Key) werden Serving und
  Processing als Kubernetes-Secret injiziert, nicht als Klartext-Env in den
  Templates.
- **PVCs:** Kafka und MinIO sind an persistente Volumes gebunden
  (`local-path`-StorageClass, je 5 Gi) — Broker-Log bzw. Lakehouse-Daten
  überleben Pod-Neustarts. Die zustandslosen Komponenten (Producer, Processing,
  Serving, UI) haben bewusst kein PVC.

### 8.3 Deklarativität und Skalierbarkeit

Jede Komponente hat in `values.yaml` einen eigenen `replicas`-Wert; Hoch- oder
Runterskalieren ist ein deklarativer Ein-Zeiler
(`kubectl scale deploy/<name> --replicas=N` oder Anpassung von `values.yaml`
und erneutem `helm upgrade`/`skaffold run`) — kein Eingriff in Code oder
Templates nötig.

### 8.4 Skalierungsnachweis

Als Nachweis wurde der Producer live von 1 auf 3 Replicas skaliert:

```
kubectl -n smartpark scale deploy smartpark-producer --replicas=3
```

Gemessen wurde der Ingest-Durchsatz direkt an den Kafka-Partitions-Offsets des
Topics `parking-events` (`kafka-get-offsets.sh`), jeweils über ein
10-Sekunden-Fenster:

| Zustand | Offset-Zuwachs / 10 s | Durchsatz |
|---|---|---|
| 1 Replica (vorher) | ~130 Events | ≈ 13 Events/s |
| 3 Replicas (nachher) | ~375 Events | ≈ 37,5 Events/s |

Der Durchsatz steigt um den Faktor **≈ 2,9** — nahezu linear mit der
Replica-Zahl. Die parallele Verarbeitung läuft über die drei Partitionen des
Topics `parking-events` als Parallelitäts-Achse.

**Beobachtung (Partition-Skew):** Da der Producer nach `zone_id` partitioniert
(Kafka-Key = `zone_id`, siehe §10), verteilen sich die fünf Zonen durch die
Hash-Partitionierung nur auf zwei der drei Partitionen — die dritte blieb in
der Messung durchgehend leer. Die Kafka-Partitionierung wurde bewusst nicht auf
eine höhere Partitionszahl ausgelegt, weil bei fünf Zonen und der gewählten
Partitionierungsstrategie mehr Partitionen den Skew nicht auflösen würden;
für eine größere Zonenzahl wäre eine feinere Partitionierung (z. B. nach
`bay_id`) die naheliegende Anpassung.

**Grenze der Skalierung — bewusst dokumentiert:** Spark (Processing) und Kafka
wurden *nicht* live mitskaliert. Die VM steht mit 12 GB RAM bereits im
Normalbetrieb unter Druck — im Betrieb kam es mehrfach zu `OOMKilled`-Ereignissen
an der Serving-API, sobald DuckDB/Delta-Operationen zusätzlichen Speicher zogen
(siehe §12). Eine zusätzliche Spark-Executor-Skalierung hätte dieses Risiko
weiter verschärft. Die Skalierung wurde daher gezielt auf die unkritischste,
zustandslose Komponente (Producer) beschränkt — ein bewusster Trade-off
zwischen Nachweis der Skalierbarkeit und Stabilität des Gesamtsystems auf
begrenzter Hardware, nicht eine technische Unmöglichkeit der Architektur.

**Architektonische Skalierbarkeit von Kafka und Spark:** Beide Komponenten sind
für horizontale Skalierung ausgelegt, auch wenn dies auf der begrenzten VM nicht
live demonstriert wurde. Das Topic `parking-events` ist mit **3 Partitionen**
angelegt — die eingebaute Parallelitäts-Achse für mehr Consumer-Durchsatz; ein
zusätzlicher Kafka-Broker (`kafka.replicas` > 1 im StatefulSet, aktuell auf 1
gesetzt) würde diese Partitionen automatisch über mehrere Broker verteilen. Der
Spark-Job läuft bewusst gedrosselt (`local[2]`, siehe Betriebs­erkenntnisse
§12), ist aber ebenso für mehr Executor-Parallelität ausgelegt: Spark verteilt
Kafka-Partitionen automatisch auf verfügbare Executor-Slots, ohne dass der
Code (`streaming_job.py`) dafür geändert werden müsste. Das Helm-Chart
unterstützt `replicas > 1` für jede Komponente gleichermaßen — die Auslegung
ist vorhanden, die Live-Demonstration wurde bewusst auf die unkritischste
Komponente beschränkt, um die im Betrieb bereits real aufgetretenen
Ressourcen-Engpässe nicht zu riskieren.


## 9. Deployment-Anleitung

Ziel: Ein Teammitglied bringt den kompletten Stack allein auf der DHBWCloud-VM
zum Laufen. Die Anleitung ist bewusst inklusive der real aufgetretenen
Stolpersteine (siehe auch §12.1) geschrieben.

### 9.1 Voraussetzungen

- **Netzzugang:** Die VM ist nur aus dem DHBW-Netz erreichbar. Von außerhalb
  zunächst per **Cisco Secure Client (VPN)** ins DHBW-Netz einwählen.
- **VM:** DHBWCloud-Instanz (`mb1.large`: 4 vCPU, 12 GB RAM, 40 GB Root-Disk,
  Ubuntu 24.04), öffentliche IPv4 `141.72.176.93`.
- **SSH-Zugang zur VM:** über das beim VM-Setup hinterlegte Keypair
  (`ssh ubuntu@141.72.176.93`). Der private Schlüssel liegt beim
  Cluster-Owner. **Weitere Teammitglieder hinzufügen:** die betreffende Person
  erzeugt sich lokal ein Schlüsselpaar (`ssh-keygen -t ed25519`), und der
  öffentliche Schlüssel wird auf der VM an `~/.ssh/authorized_keys` des
  `ubuntu`-Users angehängt.
- **Lokale Werkzeuge** (auf dem Entwickler-Rechner, unter WSL2/Ubuntu):
  `kubectl`, `helm`, `skaffold`, `docker`, Node.js 20. Ein GHCR-Login
  (`docker login ghcr.io`) mit einem Token, das `write:packages` erlaubt.

### 9.2 Versionen

| Werkzeug | Version |
|---|---|
| Kubernetes (k3s) | v1.x (k3s single-node) |
| Ubuntu (VM) | 24.04 LTS |
| Node.js (UI-Build) | 20 |
| Spark | 3.5.1 (`apache/spark:3.5.1-python3`) |
| Kafka | 3.9.0 (KRaft-Modus, ohne ZooKeeper) |
| Delta Lake | 3.1.0 |
| MinIO | Chart `minio-5.4.0` |
| DuckDB (Serving) | 1.0.0 |

### 9.3 Schritt für Schritt

**1. kubeconfig einrichten** (einmalig, auf dem Entwickler-Rechner).
Die k3s-kubeconfig von der VM holen und die Server-Adresse auf die öffentliche
IP zeigen lassen:

```bash
# auf der VM liegt sie unter /etc/rancher/k3s/k3s.yaml
scp ubuntu@141.72.176.93:/etc/rancher/k3s/k3s.yaml ~/.kube/config
# in ~/.kube/config die server-Zeile anpassen:
#   server: https://127.0.0.1:6443   ->   server: https://141.72.176.93:6443
```

Prüfen, dass die Verbindung steht:

```bash
kubectl get nodes        # Node muss "Ready" sein
```

**2. MinIO installieren** (einmalig, separates Helm-Release).
MinIO ist bewusst nicht Teil des `smartpark`-Charts, sondern ein eigenes
Infrastruktur-Release im selben Namespace:

```bash
helm repo add minio https://charts.min.io/
helm install minio minio/minio \
  --namespace smartpark --create-namespace \
  --set mode=standalone \
  --set rootUser=minioadmin --set rootPassword=minioadmin123 \
  --set persistence.size=5Gi
```

> Genauen Parametersatz ggf. an die tatsächliche Installation anpassen. Im
> Bucket `smartpark-lakehouse` legt der Spark-Job Bronze/Gold/bay_current an.

**3. Brücken-Secret für die Lakehouse-Zugangsdaten anlegen** (einmalig).
Das `smartpark`-Chart erwartet ein Secret `smartpark-minio-secret` mit den Keys
`accesskey`/`secretkey`. Ohne dieses Secret bleibt der Serving-Pod im Fehler
`CreateContainerConfigError: secret "smartpark-minio-secret" not found` hängen
(real aufgetreten, siehe §12.1):

```bash
kubectl -n smartpark create secret generic smartpark-minio-secret \
  --from-literal=accesskey=minioadmin \
  --from-literal=secretkey=minioadmin123
```

**4. Stack deployen.**

```bash
cd Cloud-Computing-Big-Data
skaffold run
```

`skaffold run` baut alle vier Images (producer, processing, serving, ui),
pusht sie nach GHCR und installiert/aktualisiert das Helm-Chart.

**5. GHCR-Packages öffentlich stellen** (einmalig, beim ersten Push je Image).
Neu angelegte GHCR-Packages sind privat; der Cluster kann sie dann nicht ziehen
(`ErrImagePull`). Auf GitHub jedes Package (`smartpark-producer`,
`smartpark-processing`, `smartpark-serving`, `smartpark-ui`) unter *Package
settings → Change visibility → Public* stellen. Danach ziehen die Pods die
Images automatisch.

**6. Hochlaufen prüfen.**

```bash
kubectl -n smartpark get pods      # alle Komponenten -> Running
```

### 9.4 Zugriff auf UI und API

Für Demo und Screenshots wird der Zugriff per **Port-Forward** hergestellt
(zwei Terminals, jeweils offen lassen):

```bash
# Terminal 1 — API
kubectl -n smartpark port-forward svc/smartpark-api 8000:8000
# Terminal 2 — UI
kubectl -n smartpark port-forward svc/smartpark-ui 8080:8080
```

Anschließend im Browser `http://localhost:8080` öffnen. Die UI liest ihre
API-URL zur Laufzeit aus der ConfigMap (`ui.apiUrl`, §8.2); für den
Port-Forward-Betrieb ist sie auf `http://localhost:8000` gesetzt.

> Hinweis zur Reproduzierbarkeit: Der Port-Forward-Zugriff setzt eine laufende
> `kubectl`-Verbindung des jeweiligen Nutzers voraus und ist damit ein
> Demo-/Entwicklungszugang. Ein dauerhafter, nutzerunabhängiger Zugang
> (NodePort oder Ingress/Traefik über die öffentliche IPv4) wäre der nächste
> Schritt für einen produktiven Betrieb (siehe Ausblick, §12.2).

### 9.5 Häufige Stolpersteine (aus unserer Erfahrung)

- **`ErrImagePull`** → GHCR-Package nicht public (Schritt 5).
- **`secret not found`** am Serving-Pod → Brücken-Secret fehlt (Schritt 3).
- **`OOMKilled` an der Serving-API** → Memory-Limit zu niedrig; im Chart auf
  768 Mi gesetzt (§12.1).
- **`DiskPressure`-Taint, alle Pods `Pending`** → Root-Disk voll; mit
  `sudo k3s crictl rmi --prune` alte Images entfernen.
- **`kubectl` läuft ins Timeout** → nicht im DHBW-VPN, oder k3s auf der VM
  gestoppt (`sudo systemctl status k3s`).


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

Da die Anwendung zur Bewertung nicht ausgeführt wird, ersetzen die folgenden
Screenshots den Funktionstest. Sie belegen das laufende System (UI, Pods,
Serving-Output) sowie Beispiel-Outputs der Pipeline und den Skalierungs-Nachweis.

### 11.1 User-facing UI im Betrieb

![SmartPark-Dashboard mit KPIs, Zonen-Übersicht und Bucht-Raster](docs/screenshots/ui_dashboard.png)

*Live-Dashboard (Rolle B): Das **LIVE**-Badge und `API: ok` oben rechts belegen
die reale Anbindung an die Pipeline (kein Mockup). Oben die aggregierten KPIs
(frei/belegt gesamt, Belegungsquote, aktive Zonen), links die Zonen-Übersicht
mit Belegungs-Ampel, rechts das Bucht-Raster der ausgewählten Zone (grün = frei,
rot = belegt) — die aus der `bay_current`-Tabelle gespeiste Einzel-Bucht-Ansicht.*

![Event-Injektor der UI](docs/screenshots/ui_injektor.png)

*Event-Injektor (Rolle A, Datenlieferant): Über Zone/Bucht/Zustand wird ein
Event erzeugt und per `POST /events` in die Pipeline eingespeist. Der Log-Eintrag
unten (`zone-4 / bay-4-0 -> FREE`) bestätigt das gesendete Event.*

![Dashboard mit veränderter Belegung](docs/screenshots/ui_belegung.png)

*Dasselbe Dashboard zu einem anderen Zeitpunkt (33 % statt 48 % Belegung,
anderer Aktualisierungs-Zeitstempel): Die Anzeige folgt den live verarbeiteten
Daten — Beleg dafür, dass die Werte tatsächlich aus der laufenden Pipeline
stammen und sich fortlaufend aktualisieren.*

### 11.2 Cluster-Status und Serving-Output

![kubectl get pods, Kafka-Offsets und Serving-JSON](docs/screenshots/system_pods_offsets_json.png)

*Ein Screenshot, drei Nachweise:*
- *`kubectl get pods` — alle Komponenten (minio, api ×2, kafka, processing,
  producer, ui) im Status **Running**.*
- *Kafka-Offsets des Topics `parking-events` — die stetig steigenden Offsets
  belegen den kontinuierlichen Event-Strom (Beispiel-Output der Ingestion).*
- *Serving-Output: die JSON-Antwort von `GET /zones/availability` mit den
  aggregierten Verfügbarkeiten je Zone (`events_total`, `occupied`, `free`).*

### 11.3 Skalierungs-Nachweis (horizontale Skalierung des Producers)

![Vorher: 1 Producer-Replica mit Offset-Zuwachs](docs/screenshots/scale_vorher.png)

*Ausgangszustand: **1** Producer-Replica. Der Offset-Zuwachs über 10 Sekunden
(~130 Events) entspricht ca. **13 Events/s**.*

![Hochskalieren des Producers von 1 auf 3 Replicas](docs/screenshots/scale_1zu3.png)

*`kubectl scale deploy smartpark-producer --replicas=3` — der Rollout bringt
**3** Producer-Pods in den Status Running.*

![Nachher: 3 Producer-Replicas mit höherem Offset-Zuwachs](docs/screenshots/scale_nachher.png)

*Nach der Skalierung: Der Offset-Zuwachs über 10 Sekunden (~375 Events)
entspricht ca. **37,5 Events/s** — ein Faktor von ~2,9, also nahezu lineare
horizontale Skalierung über die Kafka-Partitionen als Parallelitäts-Achse
(Details und Einordnung in §8.4).*


## 12. Grenzen des Prototyps und Ausblick

### 12.1 Scope-Grenzen

**Ressourcen der Ziel-VM.** Die DHBWCloud-VM (`mb1.large`, ursprünglich 10 GB,
später auf 40 GB Root-Disk erweitert, 12 GB RAM) ist die härteste reale
Grenze des Projekts und hat die Architektur an mehreren Stellen sichtbar
geprägt:

- **DiskPressure:** Im Betrieb füllte sich die Root-Disk durch wiederholte
  Image-Builds so weit, dass k3s den Node automatisch mit dem Taint
  `node.kubernetes.io/disk-pressure:NoSchedule` sperrte — alle Pods, inklusive
  Kafka, blieben `Pending`. Behoben durch `k3s crictl rmi --prune` (ungenutzte
  Images entfernen) und später durch Erweiterung der Root-Disk auf 40 GB.
- **RAM-Grenze am Serving-Pod:** Das ursprüngliche Memory-Limit der Serving-API
  (256 Mi) reichte nicht, sobald DuckDB die `delta`-Extension lud und einen
  Scan über eine große Tabelle fuhr — der Container wurde wiederholt vom
  Kernel mit `OOMKilled` (Exit Code 137) beendet. Behoben durch Anheben des
  Limits auf 768 Mi **und** durch Umbau der API, die Extension einmalig beim
  Start statt bei jeder Anfrage zu laden.
- **Wachsende Delta-Tabellen ohne Kompaktierung:** `read_parquet('*.parquet')`
  auf einer über Stunden gewachsenen Delta-Tabelle (Bronze, später auch Gold)
  musste jede jemals geschriebene Dateiversion öffnen und wurde so langsam,
  dass Anfragen in Timeouts liefen. Kein automatisches `OPTIMIZE`/Compaction
  ist im Prototyp implementiert — bewusst außerhalb des Scopes gelassen (siehe
  Ausblick). Für die abgefragten Tabellen wurde stattdessen `delta_scan()`
  eingesetzt, das nur die aktuell gültige Version liest.
- **Spark-Kafka-Consumer-Hang:** Unter Last geriet der Structured-Streaming-Job
  wiederholt in einen Zustand, in dem der Kafka-Consumer laut Log „may hang"
  (bekanntes Verhalten, referenziert als KAFKA-1894) meldete und keine neuen
  Batches mehr verarbeitete, obwohl der Pod als `Running` gemeldet wurde. Ein
  vollständiger Neustart des Processing-Deployments behob dies jeweils; eine
  automatische Erkennung/Selbstheilung dieses Zustands ist nicht implementiert.

**Funktionaler Scope.** Der Trend-Verlauf je Zone (Zeitreihe aus der
Gold-Tabelle) wurde in der UI bewusst wieder entfernt, nachdem die zugrunde
liegende Route unter der oben beschriebenen Dateizahl-Problematik litt. Die
Live-Anzeige (aktuelle Verfügbarkeit je Zone und je Bucht, aus `bay_current`)
ist davon nicht betroffen und lief im Test stabil und performant. Ein
Session-Join oder eine externe Anreicherung (z. B. Wetter- oder
Kalenderdaten) wurde nicht umgesetzt — der Prototyp konzentriert sich auf die
geforderte eine (hier: drei, siehe §5) nicht-triviale Transformation.
Exactly-once-Semantik wurde nicht explizit verifiziert; Delta-Checkpoints und
-MERGE machen Wiederanläufe idempotent, ein formaler Nachweis fehlt.

**Horizontale Skalierung.** Wie in §8.4 dargelegt, wurde die Skalierung live
nur am Producer demonstriert; Kafka und Spark sind architektonisch ebenso
skalierbar ausgelegt, wurden aber bewusst nicht mitskaliert, um die oben
beschriebenen Ressourcengrenzen der VM nicht zusätzlich zu strapazieren.

### 12.2 Ausblick

Mit mehr Zeit oder größerer Infrastruktur wären folgende Erweiterungen
naheliegend:

- **Automatische Kompaktierung** der Delta-Tabellen (periodisches `OPTIMIZE`
  bzw. `VACUUM`), damit Lesezugriffe auch über Tage/Wochen Laufzeit performant
  bleiben, ohne manuell einzugreifen.
- **Autoscaling statt manueller Skalierung**, z. B. ein HorizontalPodAutoscaler
  auf CPU-Auslastung für Producer/Serving oder KEDA auf Kafka-Consumer-Lag für
  das Processing.
- **Mehrere Kafka-Broker** (StatefulSet `replicas > 1`) mit über die
  Partitionen verteilten Consumer-Gruppen, um die in §8.4 beobachtete
  Partition-Skew (Zone-basierter Key trifft nur 2 von 3 Partitionen) aufzulösen
  — z. B. durch eine feinere Partitionierung nach `bay_id` statt `zone_id`.
- **Formaler Nachweis von Exactly-once-Verarbeitung** über gezielte
  Fehlerinjektion (Job-Neustart während eines Batches) und Prüfung auf
  Duplikate in `gold/zone_availability`.
- **Session-Join/Anreicherung** als zweite nicht-triviale Transformation, z. B.
  Verweildauer je Bucht durch Verknüpfung aufeinanderfolgender
  `OCCUPIED`/`FREE`-Events derselben `bay_id`.
- **Monitoring** (Prometheus/Grafana) statt der manuellen `kubectl
  top`/Log-Beobachtung, die im Betrieb zur Diagnose der oben genannten
  Ressourcenprobleme verwendet wurde — damit wären DiskPressure und OOM-Events
  proaktiv statt reaktiv erkennbar gewesen.

**Ausblick entlang der Big-Data-V's.** Der Prototyp erfüllt Velocity
(kontinuierlicher Event-Strom, siehe §2) und in Grenzen Volume (Skalierung des
Ingest gezeigt, §8.4); **Variety** ist im aktuellen Scope bewusst nicht
abgedeckt — es gibt genau eine strukturierte Event-Quelle (Belegungs-Events).
Bei einem Rollout auf eine ganze Stadt kämen realistisch weitere, strukturell
andersartige Datenquellen hinzu:

- **Kameradaten/Bilderkennung** an Einfahrten zur Plausibilisierung der
  Sensor-Events (unstrukturiert/binär statt der aktuellen JSON-Events) —
  würde eine zusätzliche Ingestion-Route und vermutlich eine
  Objekterkennungs-Vorverarbeitung vor Kafka nötig machen.
- **Wetter- und Kalenderdaten** (extern, batch-artig statt streaming) als
  Anreicherung der Gold-Tabelle — ein klassischer Kandidat für den in §12.1
  erwähnten Join.
- **Zahlungs-/Ticketing-Daten** aus Parkscheinautomaten, strukturiert aber mit
  anderem Schema und anderer Aktualisierungsfrequenz als die Belegungs-Events.

Eine echte Multi-Source-Pipeline mit mehreren strukturell unterschiedlichen
Eingängen — und damit eine vollständige Erfüllung von Variety — wäre der
nächste sinnvolle Ausbauschritt, sobald mehr als eine reale Datenquelle zur
Verfügung steht.

---

## Bonus: Abweichungen vom Standardweg

Mehrere Entscheidungen weichen bewusst vom in der Vorlesung gezeigten
Standardweg ab. Jede ist unten mit der technischen Umsetzung und der
Begründung aufgeführt.

| Abweichung | Umsetzung | Begründung |
|---|---|---|
| **MinIO/S3 statt HDFS** | Objektspeicher via MinIO, S3A-Connector in Spark, `delta_scan`/`httpfs` in DuckDB | Entkoppelt Storage von Compute — der Spark-Job kann neu gestartet oder (theoretisch) skaliert werden, ohne dass Daten betroffen sind. Für einen Single-Node-Prototyp auf einer ressourcenbegrenzten VM ist ein leichtgewichtiger Objektspeicher passender als ein HDFS-Cluster mit eigenem NameNode/DataNode-Overhead. |
| **Delta Lake statt reinem Parquet** | Alle drei Tabellen (Bronze/Gold/bay_current) im Delta-Format mit Transaktionslog | Ermöglicht sichere nebenläufige Schreibvorgänge mehrerer Streaming-Sinks und — zwingend für `bay_current` — Upserts per `MERGE`. Siehe §6.2 für die ausführliche Begründung inklusive der real aufgetretenen Performance-Problematik. |
| **DuckDB statt zweitem Spark-Prozess für das Serving** | FastAPI + DuckDB mit `delta_scan()` liest die Delta-Tabellen direkt aus MinIO | RAM-schonende Alternative zu einem zweiten, dauerhaft laufenden Spark-Cluster für Lesezugriffe — auf der 12-GB-VM ein spürbarer Unterschied. Wurde during des Betriebs zusätzlich optimiert, siehe §6.2/§12.1. |
| **Stateful Processing über Mindestanforderung hinaus** | Eigener `foreachBatch`-Sink mit Delta-`MERGE` (`gold/bay_current`, siehe §5.3) zusätzlich zur geforderten einen nicht-trivialen Transformation | Ermöglicht eine performante Live-Ansicht des Zustands jeder einzelnen Parkbucht, die aus der Windowed-Aggregation (Gold) allein nicht ableitbar wäre (dort ist `bay_id` wegaggregiert). Zeigt echtes Stateful-Stream-Processing über eine reine Aggregation hinaus. |
| **k3s statt Vanilla-Kubernetes** | Leichtgewichtige k3s-Distribution auf der DHBWCloud-VM | Geringerer Ressourcen-Overhead für Kubernetes selbst — auf einer VM mit 12 GB RAM relevant, da mehr Speicher für die eigentlichen Workloads (Kafka, Spark) verbleibt. |


## Weiterführende Projektdokumente

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — Branch-Strategie und Commit-Konventionen
- [`docs/TEAM.md`](docs/TEAM.md) — Arbeitspakete, Rollen, Eigenanteil-Log
- [`docs/MEILENSTEINE.md`](docs/MEILENSTEINE.md) — Abgabetermin und Meilensteinplan
