#!/usr/bin/env bash
#
# SmartPark — Repo-Bootstrap (Checkliste 0.1)
#
# Einmalig von EINER Person ausführen, danach klonen alle anderen nur noch.
#
#   chmod +x setup-repo.sh
#   ./setup-repo.sh
#
set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${BLUE}▶${NC} $*"; }
ok()    { echo -e "${GREEN}✔${NC} $*"; }
warn()  { echo -e "${YELLOW}!${NC} $*"; }

# ---------------------------------------------------------------- 1. git init
if [ -d .git ]; then
  warn "Es existiert bereits ein .git/ — überspringe git init."
else
  info "Initialisiere Git-Repository mit Default-Branch 'main' ..."
  git init -b main
  ok "Repository initialisiert."
fi

# ------------------------------------------------------- 2. Identität setzen
info "Git-Identität für dieses Repo setzen (wichtig für den Eigenanteil-Nachweis!)"
CURRENT_NAME=$(git config user.name  || true)
CURRENT_MAIL=$(git config user.email || true)

read -rp "  Dein Name  [${CURRENT_NAME:-leer}]: " GIT_NAME
read -rp "  Deine Mail [${CURRENT_MAIL:-leer}]: " GIT_MAIL
[ -n "${GIT_NAME}" ] && git config user.name  "${GIT_NAME}"
[ -n "${GIT_MAIL}" ] && git config user.email "${GIT_MAIL}"

# Rebase beim Pull → lineare, gut lesbare History
git config pull.rebase true
git config branch.autosetuprebase always
ok "Identität: $(git config user.name) <$(git config user.email)>"

# ------------------------------------------------- 3. Secret-Schutz (Vorsorge)
info "Lege einen einfachen pre-commit Hook gegen versehentliche Secrets an ..."
mkdir -p .git/hooks
cat > .git/hooks/pre-commit <<'HOOK'
#!/usr/bin/env bash
# Blockt offensichtliche Secrets, bevor sie in der History landen.
PATTERN='(BEGIN [A-Z ]*PRIVATE KEY|aws_secret_access_key|MINIO_SECRET_KEY *= *[^ ]|password *[:=] *["'"'"'][^"'"'"']{6,}|client-certificate-data:)'
if git diff --cached -U0 | grep -nEi "$PATTERN" >/dev/null; then
  echo "✖ Commit abgebrochen: möglicher Secret-Fund im Diff."
  git diff --cached -U0 | grep -nEi "$PATTERN" | head -5
  echo "  → Wert in ein k8s Secret / .env auslagern."
  echo "  → Wenn es ein Fehlalarm ist: git commit --no-verify"
  exit 1
fi
exit 0
HOOK
chmod +x .git/hooks/pre-commit
ok "pre-commit Hook aktiv."
warn "Hooks werden NICHT mitgecloned — jede Person führt dieses Skript einmal aus."

# ------------------------------------------------------- 4. Erster Commit
info "Erster Commit ..."
git add -A
if git diff --cached --quiet; then
  warn "Nichts zu committen."
else
  git commit -m "chore(repo): Projekt-Setup — gitignore, Branch-Strategie, Team- und Meilensteinplan

Setzt Punkt 0.1 der Umsetzungs-Checkliste um:
- .gitignore (Python, Node, k8s-Secrets, Ansible, Spark/Delta-Laufzeitdaten)
- CONTRIBUTING.md: main + Feature-Branches, Conventional Commits, PR-Workflow
- docs/TEAM.md: Aufgabenverteilung AP1-AP6, Rollen, Kommunikationskanaele
- docs/MEILENSTEINE.md: Abgabetermin und Rueckwaertsplanung M0-M6
- .github/CODEOWNERS + PR-Template"
  ok "Commit erstellt."
fi

# ------------------------------------------------------------- 5. Remote
echo
info "Remote verbinden (Repo vorher auf GitHub/GitLab anlegen — OHNE README/gitignore!)"
read -rp "  Remote-URL (leer = überspringen): " REMOTE_URL
if [ -n "${REMOTE_URL}" ]; then
  git remote remove origin 2>/dev/null || true
  git remote add origin "${REMOTE_URL}"
  git push -u origin main
  ok "Nach ${REMOTE_URL} gepusht."
else
  warn "Übersprungen. Später:  git remote add origin <URL> && git push -u origin main"
fi

# ------------------------------------------------------------- 6. Nächste Schritte
cat <<'EOF'

──────────────────────────────────────────────────────────────
 Punkt 0.1 fast durch. Was noch von Hand zu tun ist:
──────────────────────────────────────────────────────────────

 1. Repo auf GitHub/GitLab: Branch-Schutz für 'main' aktivieren
    Settings → Branches → Add rule:
      • Require a pull request before merging (1 Approval)
      • Do not allow bypassing
    → sichert die "plausible Git-History" (5 P. Eigenanteil)

 2. Alle Teammitglieder als Collaborator einladen

 3. docs/TEAM.md ausfüllen: Namen, Handles, AP-Zuordnung,
    Gruppennummer, Kommunikationskanal + Weekly-Termin

 4. docs/MEILENSTEINE.md ausfüllen: Abgabetermin eintragen,
    M0–M6 rückwärts datieren

 5. .github/CODEOWNERS: @handle1..4 durch echte Handles ersetzen

 6. Kommunikationskanal anlegen (Discord/Slack/Teams) und
    Invite-Link in docs/TEAM.md eintragen

 7. Jede Person klont und führt dieses Skript einmal aus
    (setzt eigene Identität + Secret-Hook)

 Danach: Checkliste 0.2 — Repo-Skelett
──────────────────────────────────────────────────────────────
EOF
