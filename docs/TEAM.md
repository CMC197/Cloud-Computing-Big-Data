# Team, Rollen und Aufgabenverteilung

> Grundlage für das Kriterium *„Reflexion und Eigenanteil"*. Eine gekürzte
> Fassung steht in §12 der README.

---

## 1. Teammitglieder

| # | Name | Schwerpunkt |
|---|---|---|
| 1 | Philip | Implementierung (Serving, UI, Processing) & technische Doku |
| 2 | Leo | Implementierung (Infrastruktur, Pipeline) & technische Doku |
| 3 | Niklas | Use-Case & Datenmodell, Recherche, fachlicher Berichtsteil |
| 4 | Nikolay | Projektplanung & Organisation, Testing, Qualitätssicherung |

---

## 2. Aufgabenverteilung

Die vier Aufgabenbereiche wurden entlang der Stärken im Team verteilt und bauen
aufeinander auf: Die konzeptionelle Vorarbeit (Use Case, Architektur-Recherche,
Planung) bildete die Grundlage, auf der die technische Umsetzung (Code +
Deployment) aufsetzte; Testing und Qualitätssicherung liefen begleitend zurück
in die Umsetzung.

| Bereich | Inhalt | Hauptverantwortlich | Mitarbeit |
|---|---|---|---|
| **Use Case & Big-Data-Konzept** | Ausarbeitung des Parking-Use-Cases, Big-Data-Begründung (V's), Anforderungsanalyse | Niklas | Nikolay |
| **Architektur-Recherche** | Vergleich Kappa/Lambda, Technologie-Auswahl (Kafka, Spark, Delta, MinIO), Bewertung von Alternativen | Niklas | Philip, Leo |
| **Projektplanung & Organisation** | Zeitplan, Meilensteine, Koordination, Abgabe-Organisation, Verwaltung der DHBWCloud-Ressourcen | Nikolay | alle |
| **Ingestion & Pipeline-Infrastruktur** | Kafka (Topic, Partitionen, Retention), Producer, k3s/Helm/Skaffold, Deployment auf DHBWCloud | Leo | Philip |
| **Stream Processing** | Spark Structured Streaming: Windowing, Watermark, Stateful Merge (bay_current) | Leo | Philip |
| **Storage** | MinIO + Delta Lake, Bronze/Gold/bay_current, Schema-Design | Leo | Niklas |
| **Serving & UI** | FastAPI + DuckDB (delta_scan), React-Dashboard, Event-Injektor | Philip | Leo |
| **Betrieb & Fehlerbehebung** | Diagnose/Behebung der Ressourcen- und Deployment-Probleme (DiskPressure, OOM, delta_scan) | Philip, Leo | Nikolay |
| **Testing & Qualitätssicherung** | Manuelles Testen von UI und Endpunkten, Nachvollziehen des End-to-End-Durchstichs, Screenshots | Nikolay | Niklas |
| **Bericht (README)** | fachliche Abschnitte §1–§2: Niklas · technische Abschnitte §3–§12: Philip, Leo · Screenshots §11: Nikolay | alle | — |

### README-Abschnitte → Verantwortlich

| README-§ | Thema | Verantwortlich |
|---|---|---|
| §1, §2 | Use Case, Datencharakteristik (V's) | Niklas |
| §3, §4 | Architektur + Diagramm, Komponenten & Datenfluss | Philip, Leo (Recherche: Niklas) |
| §5 | Processing-Logik | Leo, Philip |
| §6 | Speicherkonzept | Leo |
| §7 | User-facing UI | Philip |
| §8, §9 | k8s-Deployment, Deployment-Anleitung | Leo, Philip |
| §10 | Wesentliche Codeabschnitte | Philip, Leo |
| §11 | Screenshots & Nachweise | Nikolay |
| §12 | Grenzen & Ausblick, Eigenanteil | alle |

---

## 3. Anmerkung zur Git-History

Der Großteil der Git-Commits stammt von den beiden Mitgliedern mit
Implementierungs-Schwerpunkt (Philip, Leo), da Code und README über Git
versioniert wurden. Die konzeptionelle, planerische und qualitätssichernde
Arbeit von Niklas und Nikolay (Use-Case- und Architektur-Ausarbeitung,
Recherche, Organisation, Testing) schlägt sich naturgemäß weniger in Commits
nieder, floss aber inhaltlich in den Prototyp und den Bericht ein —
insbesondere in die Use-Case-/V's-Abschnitte (§1, §2), die
Architektur-Entscheidung (§3) und die Verifikation des laufenden Systems (§11).

---

## 4. Kommunikation

| Kanal | Zweck |
|---|---|
| `<Discord / WhatsApp / Teams>` | Tagesgeschäft, schnelle Abstimmung, Blocker |
| GitHub | Code, README, Nachvollziehbarkeit |
