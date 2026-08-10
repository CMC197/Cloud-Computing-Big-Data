# Zusammenarbeit im SmartPark-Repo

> Verbindliche Arbeitsweise für alle Teammitglieder.
> **Warum das zählt:** Das Kriterium *„Reflexion und Eigenanteil" (5 Punkte)* verlangt eine
> **plausible Git-History**. Wer nur einmal am Ende alles committet, verschenkt Punkte —
> und das Team riskiert, dass der Eigenanteil nicht nachweisbar ist.

---

## 1. Branch-Strategie

Wir arbeiten mit **`main` + kurzlebigen Feature-Branches** (GitHub-Flow). Kein `develop`,
kein Git-Flow — dafür ist das Projekt zu kurz (7 Wochen) und das Team zu klein.

```
main ──●────●────●────●────●────●──────────►  immer deploybar
        \        /      \        /
         ●──●──●         ●──●──●
      feat/ap1-kafka   feat/ap2-windowing
```

| Regel | Warum |
|---|---|
| `main` ist immer deploybar (`skaffold run` läuft durch) | Sonst blockiert ein kaputter `main` das ganze Team |
| **Nie direkt auf `main` pushen** — immer über Pull Request | Vier-Augen-Prinzip, und der PR dokumentiert den Eigenanteil |
| Feature-Branch lebt **max. 3 Tage** | Lange Branches = Merge-Hölle bei einem Mono-Repo |
| Branch wird nach dem Merge gelöscht | Übersicht |
| Vor dem Push: `git pull --rebase origin main` | Lineare History, leichter zu bewerten |

### Branch-Namen

```
<typ>/<ap>-<kurzbeschreibung>
```

| Typ | Verwendung | Beispiel |
|---|---|---|
| `feat/` | Neues Feature | `feat/ap2-session-join` |
| `fix/` | Bugfix | `fix/ap5-kafka-oom-limits` |
| `docs/` | README, Diagramme, Screenshots | `docs/ap6-architekturdiagramm` |
| `chore/` | Setup, Tooling, Dependencies | `chore/ap5-skaffold-profile` |
| `spike/` | Zeitlich begrenztes Experiment, darf weggeworfen werden | `spike/spark-delta-minio` |

Beispiele:
```bash
git switch -c feat/ap1-kafka-producer
git switch -c fix/ap4-api-cors
git switch -c spike/flink-vs-spark-memory
```

---

## 2. Commit-Konvention

Wir nutzen **Conventional Commits** — kurz, aber aussagekräftig:

```
<typ>(<scope>): <was wurde geändert, im Imperativ>
```

**Scopes** entsprechen den Arbeitspaketen bzw. Ordnern:
`producer` · `processing` · `serving` · `ui` · `helm` · `ansible` · `docs` · `skaffold`

Gute Commits:
```
feat(producer): Simulator erzeugt Enter-/Exit-Events mit Zonen-ID
feat(processing): Tumbling-Window-Aggregation über 5 Min mit Watermark
fix(helm): Memory-Limit für Kafka auf 1Gi gesetzt (OOMKilled auf DHBWCloud)
docs(readme): §3 Kappa-vs-Lambda-Begründung ergänzt
chore(ansible): k3s-Playbook um Traefik-Ingress-Config erweitert
```

Schlechte Commits (bitte vermeiden):
```
update
fix
stuff
wip
Änderungen
```

**Faustregel:** Mindestens **1 sinnvoller Commit pro Arbeitstag**, an dem am Projekt
gearbeitet wurde. Lieber viele kleine als ein Riesen-Commit.

---

## 3. Identität korrekt setzen (einmalig, wichtig!)

Ohne korrekten Namen/E-Mail taucht euer Eigenanteil in der History nicht auf.

```bash
git config user.name  "Vorname Nachname"
git config user.email "vorname.nachname@student.dhbw-mannheim.de"

# Prüfen, dass alles stimmt:
git log --format='%an <%ae>' | sort | uniq -c
```

> Bei mehreren Autoren an einem Commit (Pair Programming):
> `Co-authored-by: Name <email>` als letzte Zeile der Commit-Message.

---

## 4. Pull-Request-Workflow

1. Branch von aktuellem `main` abzweigen
2. Arbeiten, in kleinen Schritten committen
3. `git pull --rebase origin main`, Konflikte lösen
4. Push + PR öffnen (Template wird automatisch geladen)
5. **Ein anderes Teammitglied** reviewt — mindestens: läuft es? ist es verständlich?
6. Merge per **Squash** (bei vielen kleinen Fixup-Commits) oder **Rebase-Merge**
7. Branch löschen

**Review-Pflicht ist keine Bürokratie:** Jede Person muss am Ende erklären können,
wie das Gesamtsystem funktioniert (potenzielle Rückfragen in der Bewertung).

---

## 5. Was NIEMALS committet wird

- MinIO Access-/Secret-Key, Passwörter, Tokens → gehören in **k8s Secrets** bzw. `.env` (gitignored)
- `kubeconfig` / `k3s.yaml` von der DHBWCloud-VM
- SSH-Private-Keys
- `node_modules/`, `__pycache__/`, `.venv/`
- Delta-/Kafka-Laufzeitdaten, Spark-Checkpoints

Vor dem Commit prüfen:
```bash
git status
git diff --cached          # was landet wirklich im Commit?
```

Wenn doch mal ein Secret durchrutscht: **sofort im Team melden**, Key rotieren.
Ein `git rm` reicht nicht — der Key steht noch in der History.

---

## 6. Merge-Konflikte vermeiden (Mono-Repo!)

Da alle im selben Repo arbeiten:

- **Ownership respektieren**: siehe [`.github/CODEOWNERS`](.github/CODEOWNERS) — wer AP2 hat, editiert `processing/`
- Gemeinsame Dateien (`README.md`, `deploy/helm/values.yaml`, `skaffold.yaml`) sind **Konflikt-Hotspots**:
  - Vorher im Chat ankündigen: „Ich fasse gleich values.yaml an"
  - In der README: jede\*r schreibt **nur die eigenen Abschnitte**, nicht querbeet
- Oft rebasen statt einmal am Ende

---

## 7. Definition of Done pro PR

Ein PR ist fertig, wenn:

- [ ] Code läuft lokal bzw. auf dem k3s-Cluster
- [ ] Keine Secrets, keine Build-Artefakte im Diff
- [ ] Commit-Messages folgen der Konvention
- [ ] Falls relevant: README-Abschnitt aktualisiert
- [ ] Falls relevant: Screenshot in `docs/screenshots/` abgelegt
- [ ] Review durch ein anderes Teammitglied
