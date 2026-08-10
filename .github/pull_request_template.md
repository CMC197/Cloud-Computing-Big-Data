## Was ändert dieser PR?

<!-- 2–3 Sätze. Was, und vor allem: warum. -->

**Arbeitspaket:** AP<!-- 1–6 -->
**Meilenstein:** M<!-- 0–6 -->

## Wie getestet?

<!-- z.B. "skaffold dev, Event über UI eingespeist, in Gold-Tabelle sichtbar" -->

## Checkliste

- [ ] Läuft lokal bzw. auf dem k3s-Cluster
- [ ] **Keine Secrets, keine Keys, keine kubeconfig im Diff**
- [ ] Keine Build-Artefakte (`node_modules/`, `__pycache__/`, `dist/`)
- [ ] Commit-Messages folgen der Konvention (`typ(scope): ...`)
- [ ] Betroffener README-Abschnitt aktualisiert (falls relevant)
- [ ] Screenshot in `docs/screenshots/` abgelegt (falls relevant)
- [ ] Resource Requests/Limits gesetzt (bei neuen k8s-Workloads — DHBWCloud hat wenig RAM!)

## Screenshots / Output

<!-- Bei UI-, Pipeline- oder k8s-Änderungen: gleich hier einfügen.
     Die brauchen wir später sowieso für §11 der README. -->
