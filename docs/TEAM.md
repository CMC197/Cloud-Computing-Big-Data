# Team, Rollen und Aufgabenverteilung

> **Bewertungsrelevant:** Kriterium *„Reflexion und Eigenanteil" (5 Punkte)* — klare
> Aufgabenverteilung + plausible Git-History. Dieses Dokument wandert am Ende
> (gekürzt) in **§12 der README**.
>
> 🔴 **AUSFÜLLEN:** Alle `<...>`-Platzhalter durch echte Namen/Werte ersetzen.

---

## 1. Teammitglieder

| # | Name | GitHub-Handle | E-Mail (DHBW) | Haupt-AP |
|---|---|---|---|---|
| 1 | `<Name 1>` | `<@handle1>` | `<...@student.dhbw-mannheim.de>` | AP1 + AP3 |
| 2 | `<Name 2>` | `<@handle2>` | `<...>` | AP2 |
| 3 | `<Name 3>` | `<@handle3>` | `<...>` | AP4 |
| 4 | `<Name 4>` | `<@handle4>` | `<...>` | AP5 |

**Gruppennummer:** `<X>` → ZIP heißt am Ende `SmartPark_Gruppe_<X>.zip`

> **Anpassung bei anderer Teamgröße:**
> - **3 Personen:** AP5 (DevOps) ist am aufwändigsten → AP1+AP3 zu Person 1, AP2 zu Person 2, AP4+AP5 aufteilen; AP5 kriegt Unterstützung von Person 1 nach M1.
> - **5 Personen:** AP1 und AP3 trennen (Person 1 = Kafka/Producer, Person 5 = MinIO/Delta/Schema).

---

## 2. Arbeitspakete

| AP | Inhalt | Owner | Ordner | Deliverable / Definition of Done |
|---|---|---|---|---|
| **AP1** | **Ingestion** — Kafka-Setup, Topic-Design (Partitionen, Keys, Retention), Producer/Event-Simulator | `<Name 1>` | `producer/` | Laufender Event-Strom, sichtbar im Kafka-Topic (Screenshot) |
| **AP2** | **Processing** — Spark Structured Streaming: Windowing, Session-Join, Enrichment, Watermarks, Late Data | `<Name 2>` | `processing/` | Gold-Tabelle wird kontinuierlich befüllt |
| **AP3** | **Storage** — MinIO + Delta Lake, Bronze/Gold-Schichten, Partitionierung, Schema | `<Name 1>` | `processing/`, `data/` | Bronze/Gold im Lakehouse abfragbar |
| **AP4** | **Serving + UI** — FastAPI, Query-Layer (DuckDB/Trino), React-Dashboard + Event-Injektor | `<Name 3>` | `serving/`, `ui/` | End-to-End über die UI sichtbar (kein Mockup!) |
| **AP5** | **DHBWCloud / DevOps** — VM + k3s per Ansible, Skaffold, Helm-Chart, ConfigMaps/Secrets/PVCs, HPA/KEDA, Traefik-Ingress | `<Name 4>` | `ansible/`, `deploy/helm/`, `skaffold.yaml` | `skaffold run` bringt den kompletten Stack auf der DHBWCloud hoch |
| **AP6** | **Bericht** — README (12 Pflichtabschnitte), Architekturdiagramm, Screenshots, Bonus-Begründungen | **alle**, koordiniert von `<Name ?>` | `README.md`, `docs/` | Vollständige README + eingebettete Nachweise |

**Kritischer Pfad:** AP1 → AP2 → AP3 (Pipeline-Rückgrat).
AP4 und AP5 laufen teilweise parallel, sobald der dünne Durchstich (M1) steht.

> **AP6 ist Gemeinschaftsaufgabe:** Jede Person schreibt die README-Abschnitte zu
> ihrem eigenen AP. Der/die Koordinator\*in sorgt für Konsistenz, Diagramm und
> Vollständigkeit. **Fehlender Pflichtabschnitt = 0 Punkte im Kriterium.**

### README-Abschnitte → Owner

| README-§ | Thema | Punkte | Owner |
|---|---|---|---|
| §1, §2 | Use Case, Datencharakteristik (V's mit Zahlen) | 10 | `<Name ?>` |
| §3, §4 | Architekturentscheidung Kappa/Lambda + Diagramm, Komponenten & Datenfluss | 20 | `<Name ?>` |
| §5 | Processing-Logik | 20 | AP2-Owner |
| §6 | Speicherkonzept | 10 | AP3-Owner |
| §7 | User-facing UI | 10 | AP4-Owner |
| §8, §9 | k8s-Deployment, Deployment-Anleitung | 25 | AP5-Owner |
| §10, §11 | Codeabschnitte verlinkt, Screenshots | (Teil von 10) | alle |
| §12 | Grenzen & Ausblick, Eigenanteil | 5 | Koordinator\*in |

---

## 3. Rollen (zusätzlich zu den APs)

| Rolle | Person | Aufgabe |
|---|---|---|
| **Repo-Owner** | `<Name ?>` | Branch-Schutz, Rechte, Merge im Zweifel |
| **Cluster-Owner** | AP5-Owner | kubeconfig verteilen, VM am Leben halten, Ressourcen im Blick |
| **Bericht-Koordination** | `<Name ?>` | README-Konsistenz, Deadline-Wächter\*in, ZIP bauen |

---

## 4. Kommunikation

| Kanal | Zweck | Link / ID |
|---|---|---|
| **`<Discord / Slack / Teams>`** | Tagesgeschäft, schnelle Fragen | `<Invite-Link>` |
| Channel `#allgemein` | Orga, Termine | |
| Channel `#dev` | Technische Fragen, Fehlermeldungen | |
| Channel `#deployment` | Cluster-Status, „Cluster ist down", kubeconfig-Änderungen | |
| GitHub Issues / PRs | Alles Nachvollziehbare (wird mitbewertet!) | `<Repo-URL>` |

**Feste Termine**

| Was | Wann | Dauer |
|---|---|---|
| Weekly Sync (Meilenstein-Review, Blocker) | `<Wochentag, Uhrzeit>` | 30 min |
| Kurzes Standup (async im Chat: gestern / heute / Blocker) | Mo–Fr `<Uhrzeit>` | 5 min |

**Erreichbarkeitsregel:** Blocker werden **sofort** im Chat gemeldet, nicht bis zum
Weekly aufgespart. Wer 24 h blockiert ist, meldet sich — das Projekt ist zu kurz für
stilles Feststecken.

---

## 5. Eigenanteil-Log

Jede Person führt hier ein **kurzes** laufendes Log. Fließt am Ende in **§12 der README**.
Das kostet 2 Minuten pro Woche und sichert 5 Punkte.

### `<Name 1>` — AP1 (Ingestion) + AP3 (Storage)
| KW | Was gemacht | Commits / PRs |
|---|---|---|
| | | |

### `<Name 2>` — AP2 (Processing)
| KW | Was gemacht | Commits / PRs |
|---|---|---|
| | | |

### `<Name 3>` — AP4 (Serving + UI)
| KW | Was gemacht | Commits / PRs |
|---|---|---|
| | | |

### `<Name 4>` — AP5 (DHBWCloud / DevOps)
| KW | Was gemacht | Commits / PRs |
|---|---|---|
| | | |

---

## 6. Offene Team-Entscheidungen

Diese Punkte sind laut Projektplan noch offen und müssen **gemeinsam** entschieden
werden — die Begründung wandert direkt in die README:

| Entscheidung | Optionen | Deadline | Status |
|---|---|---|---|
| Single-Node vs. Multi-Node k3s | 1 Server / 1 Server + 1–2 Agents | nach Flavor-Check (Wo. 1) | ⬜ offen |
| Container-Registry | Forgejo in-cluster (Kurs) / GHCR / Docker Hub | Wo. 1 | ⬜ offen |
| Streaming-Engine | Spark (Standard) / Flink (weniger RAM → Bonus) | nach Spike (Wo. 1) | ⬜ offen |
| Query-Layer | DuckDB (leicht) / Trino (näher am Lehrstoff) | Wo. 3 | ⬜ offen |
| Zweite Variety-Quelle | ja / nein | Wo. 4 | ⬜ offen |
