# DS2 — Der souveräne Lakehouse-Agent

Trino MCP-Server + lokales Ollama-Modell + Eval-Harness fuer den Data-&-Analytics-Hackathon
(Gruppe DS2). Siehe [FINDINGS.md](FINDINGS.md) fuer die Ergebnisse.

## Setup

```bash
# Ollama (macOS/Homebrew)
brew install ollama
brew services start ollama
ollama pull qwen2.5:7b
ollama pull qwen2.5:1.5b

# Trino MCP-Server (tuannvm/mcp-trino) — Homebrew-Tap ist zum Zeitpunkt der Erstellung
# kaputt (zeigt auf eine 404-Release), daher das offizielle Install-Skript:
curl -fsSL https://raw.githubusercontent.com/tuannvm/mcp-trino/main/install.sh | \
  bash -s -- --no-config
# --no-config ist wichtig: ohne das Flag registriert sich der Server automatisch in
# Claude Code/Claude Desktop, was hier nicht gewuenscht ist.
# Installiert nach ~/.local/bin/mcp-trino — ggf. PATH ergaenzen:
export PATH="$HOME/.local/bin:$PATH"

# ollmcp (interaktive Bridge mit Human-in-the-Loop, fuer explorative Sessions)
uv tool install --upgrade ollmcp
```

Voraussetzung: der mini-lakehouse-Stack laeuft (`docker compose up -d`) und ist befuellt
(`make seed`, plus fuer die ESG-Pipeline `bash scripts/demo2-state.sh raw_trusted` nach
einmaligem `docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt deps"`).

## Interaktive Nutzung (ollmcp)

```bash
export PATH="$HOME/.local/bin:$PATH"
ollmcp mcp add trino \
  -e TRINO_HOST=localhost -e TRINO_PORT=8080 -e TRINO_USER=mcp_agent \
  -e TRINO_CATALOG=nessie -e TRINO_SCHEMA=trusted \
  -e TRINO_SSL=false -e TRINO_SCHEME=http \
  -e TRINO_MAX_ROWS=1000 -e TRINO_QUERY_TIMEOUT=30 \
  -- "$(which mcp-trino)"

ollmcp --model qwen2.5:7b
```

`TRINO_SSL`/`TRINO_SCHEME` muessen explizit gesetzt werden — der Server nimmt sonst HTTPS
an, dieser Trino laeuft aber ohne TLS. `TRINO_ALLOW_WRITE_QUERIES` bewusst NICHT setzen —
das haelt den Server read-only (Server-seitig erzwungen, nicht nur eine Empfehlung).

Registrierung erfolgt mit `local`-Scope (`~/.config/ollmcp/mcp.local.json`, nicht
Teil des Git-Repos) — der obige Befehl hardcoded den absoluten Binary-Pfad, jede·r
muss ihn einmalig selbst ausfuehren.

## Automatisierte Auswertung (Eval-Harness)

`ollmcp`s TUI liess sich nicht zuverlaessig automatisieren (siehe FINDINGS.md, Abschnitt 3)
— `eval_harness.py` spricht MCP-Server und Ollama daher direkt an, ohne die TUI.

```bash
cd mcp_agent
uv run --project .. python eval_harness.py --models qwen2.5:7b qwen2.5:1.5b --repeats 3
```

Optionen: `--limit N` (nur die ersten N Fragen, fuer einen schnellen Testlauf),
`--models <name> [<name> ...]`, `--questions <pfad>` (eigener Fragenkatalog),
`--repeats N` (Wiederholungen pro Frage/Modell — empfohlen ≥3, Ollamas Default-Sampling
ist nicht deterministisch).

Ergebnisse: `results/results_<timestamp>.json` (vollstaendiger Trace jeder Frage) und
`.md` (Zusammenfassungstabelle).

## Dateien

- `mcp_stdio_client.py` — minimaler MCP-Client ueber stdio (handgeschrieben, keine neue
  Dependency; das Protokoll ist schlichtes zeilenweises JSON-RPC 2.0)
- `questions.json` — 18 Fragen mit Ground Truth (per direktem SQL ermittelt) und
  `check_strings`/`check_mode` fuer die automatische Grobauswertung
- `eval_harness.py` — Runner: fuehrt jede Frage gegen jedes Modell aus, protokolliert
  Tool-Aufrufe, erkennt "erzaehlt einen Tool-Call statt ihn auszufuehren" und nudged einmal
  nach, wertet grob automatisch aus (`answer_check` + `grounded_check` -> `auto_check`)
- `results/` — Lauf-Ergebnisse (JSON + Markdown)
- `FINDINGS.md` — Ergebnisse und Interpretation
