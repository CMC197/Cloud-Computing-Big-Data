# Abgabetermin & Meilensteine (Rückwärtsplanung)

> 🔴 **AUSFÜLLEN:** Abgabetermin eintragen, dann die Datumsspalte rückwärts befüllen.
> Prinzip: **erst dünner End-to-End-Durchstich, dann verbreitern** — Risiko früh senken.

---

## Abgabetermin

| | |
|---|---|
| **Offizielle Deadline** | `<TT.MM.JJJJ, HH:MM Uhr>` |
| **Team-interne Deadline** | `<Deadline − 2 Tage>` — Puffer für ZIP, Probe-Entpacken, Panik |
| **Abgabeform** | Eine einzige ZIP: `SmartPark_Gruppe_<X>.zip` |
| **Abgabekanal** | `<Moodle / Mail / ...>` |

> ⚠️ Die **team-interne Deadline** ist die, die zählt. Erfahrungsgemäß fehlen am letzten
> Tag immer noch Screenshots oder ein README-Abschnitt — und ein fehlender
> Pflichtabschnitt kostet die vollen Punkte des Kriteriums.

---

## Meilensteine (7 Wochen, rückwärts geplant)

Rechnung: **M6 = Deadline**, jede Woche eins zurück.

| # | Woche | Zeitraum | Meilenstein | Definition of Done | Haupt-AP |
|---|---|---|---|---|---|
| **M0** | 1 | `<...>` – `<...>` | **DHBWCloud-Setup & Durchstich-Skelett** | VPN für alle, VM(s) erstellt, k3s per Ansible läuft, Skaffold eingerichtet, Spark+Delta+MinIO-Spike erfolgreich, Repo + Skelett stehen | AP5 |
| **M1** | 2 | `<...>` – `<...>` | **End-to-End dünn** | 1 Event: UI → Kafka → Spark → Delta → API → UI sichtbar | AP1–AP4 |
| **M2** | 3 | `<...>` – `<...>` | **Processing-Tiefe** | Windowed Aggregation + Session-Join + Enrichment + Watermarks fertig | AP2 |
| **M3** | 4 | `<...>` – `<...>` | **Storage & Serving solide** | Bronze/Gold, Partitionierung, Query-Layer, Dashboard mit Trends | AP3, AP4 |
| **M4** | 5 | `<...>` – `<...>` | **Kubernetes & Skalierung** | Helm-Chart komplett, HPA/KEDA, Skalierung nachweisbar (Screenshots!) | AP5 |
| **M5** | 6 | `<...>` – `<...>` | **Bonus + Härtung** | Exactly-once, Schema-Evolution, ggf. CI/CD; Fehlerfälle getestet | alle |
| **M6** | 7 | `<...>` – **Deadline** | **Bericht & Abgabe** | README vollständig, Screenshots eingebettet, ZIP gebaut, Probe-Entpacken gemacht | AP6 |

---

## Harte Regeln

1. **M0 hat absolute Priorität.** Ohne laufenden k3s-Cluster auf der DHBWCloud kann
   niemand im Team arbeiten. Wenn M0 rutscht, rutscht alles.
2. **Kein Bonus vor M4.** Die 100 Kernpunkte kommen zuerst; Bonus ist gedeckelt bei
   +10 und die Maximalpunktzahl bleibt trotzdem 100.
3. **Screenshots werden mitlaufend gemacht**, nicht am Ende rekonstruiert. Besonders
   die Skalierungs-Screenshots (`kubectl get pods` / `get hpa` **vor und nach** Last) —
   die sind in Woche 7 nicht mehr nachstellbar, wenn der Cluster zickt.
4. **Ab M4 wird die README wöchentlich fortgeschrieben**, nicht in Woche 7 geschrieben.

---

## Wöchentlicher Check (im Weekly Sync)

- [ ] Meilenstein dieser Woche erreicht? Wenn nein: **was wird gestrichen**, nicht „wir holen auf"
- [ ] Blocker aus [`TEAM.md` §6](TEAM.md) entschieden?
- [ ] Jede Person hat diese Woche committet? (`git shortlog -sn --since="1 week ago"`)
- [ ] Screenshots dieser Woche in `docs/screenshots/` abgelegt?
- [ ] Eigenanteil-Log aktualisiert?

---

## Risiko-Trigger (wann wird der Plan geändert?)

| Wenn … | dann … |
|---|---|
| Ende Woche 1 läuft kein k3s | Fallback `k3sup` statt Ansible; Ansible später nachziehen |
| Spark wird auf dem VM-Flavor OOMKilled | Auf Flink umschwenken — ist als begründete Abweichung **bonusfähig** |
| Ende Woche 2 kein Durchstich | Scope kürzen: Session-Join streichen, nur Windowing + Enrichment |
| Woche 5 keine Skalierung nachweisbar | Minimal-Nachweis reicht: API-HPA 2→3 Replicas mit Screenshot |
| Woche 6 zeitlich eng | Bonus komplett streichen, direkt in M6 |
