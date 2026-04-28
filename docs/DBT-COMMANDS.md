# dbt-Befehlsreferenz fuer mini-lakehouse

Stand: 2026-04-28
Zweck: Vollstaendige Liste aller `dbt run`-, `dbt snapshot`- und
`dbt test`-Befehle, die im aktuellen Repo-Stand sinnvoll und
ausfuehrbar sind.

> **Aufruf-Konvention:** dbt ist nur im `jupyter`-Container installiert.
> Alle Aufrufe gehen ueber
> `docker compose exec jupyter bash -c "cd /home/jovyan/dbt && <dbt-Befehl>"`.
> Die Befehle unten enthalten den Wrapper bereits.

---

## Aktuelle Resource-Inventur

| Typ          | Name                       | Schema       | Status     |
|--------------|----------------------------|--------------|------------|
| Source       | `raw.cdp_emissions`        | nessie.raw   | enabled    |
| Source       | `raw.nzdpu_emissions`      | nessie.raw   | enabled    |
| Source       | `raw.fondspreise`          | nessie.raw   | enabled    |
| Source       | `raw.fund_master`          | nessie.raw   | enabled    |
| Source       | `raw.fund_positions`       | nessie.raw   | enabled    |
| Source       | `raw.owid_co2_countries`   | nessie.raw   | enabled    |
| Model        | `stg_fondspreise`          | staging      | enabled    |
| Model        | `stg_cdp_emissions`        | staging      | enabled    |
| Model        | `stg_nzdpu_emissions`      | staging      | enabled    |
| Model        | `curated_companies`        | curated      | enabled    |
| Model        | `curated_esg_emissions`    | curated      | enabled    |
| Model        | `trusted_esg_emissions`    | trusted      | enabled    |
| Snapshot     | `snp_fondspreise_scd2`     | curated      | enabled    |

---

## Voraussetzungen

```bash
# Stack hochfahren
docker compose up -d
docker compose ps

# Packages installieren (einmalig nach Repo-Clone oder Package-Aenderung)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt deps"

# Manifest pruefen / Syntaxcheck
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt parse"
```

---

## Globale Befehle

### Alles bauen + testen

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build"
```

`dbt build` baut Modelle und fuehrt direkt im Anschluss alle
zugehoerigen Tests aus — fuer einen Komplett-Refresh nach Daten-
oder Modell-Aenderung.

> **Hinweis:** `dbt build` schliesst auch Snapshots ein. Wenn der
> Source-Stand seit dem letzten Snapshot nicht geaendert wurde,
> ist der Snapshot ein No-Op.

### Nur Modelle bauen (ohne Tests, ohne Snapshots)

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run"
```

### Nur alle Tests laufen lassen

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test"
```

---

## Demo 1 — Fondspreis-Pipeline

### Staging

```bash
# Build
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_fondspreise"

# Test
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select stg_fondspreise"
```

### Source-Tests fuer Raw-Fondspreise

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.fondspreise"
```

### SCD2-Snapshot

```bash
# Snapshot bauen / aktualisieren
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt snapshot --select snp_fondspreise_scd2"

# Snapshot-Tests
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select snp_fondspreise_scd2"
```

### Komplett-Pipeline (Source-Tests + Staging + Snapshot + Tests)

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select source:raw.fondspreise+"
```

Erwartete Stats nach Standard-Demo-Reset: **PASS=19/19**.

---

## Demo 2 — ESG-Pipeline

### Source-Tests fuer Raw-ESG-Tabellen

```bash
# Beide Quellen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.cdp_emissions source:raw.nzdpu_emissions"

# Einzeln
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.cdp_emissions"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.nzdpu_emissions"
```

### Staging

```bash
# Beide ESG-Staging-Modelle bauen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_cdp_emissions stg_nzdpu_emissions"

# Einzeln bauen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_cdp_emissions"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_nzdpu_emissions"

# Tests einzeln
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select stg_cdp_emissions"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select stg_nzdpu_emissions"

# Beide gemeinsam testen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select stg_cdp_emissions stg_nzdpu_emissions"
```

### Curated

```bash
# Beide Curated-Modelle bauen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select curated_companies curated_esg_emissions"

# Einzeln bauen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select curated_companies"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select curated_esg_emissions"

# Tests beider Modelle (inkl. relationships zwischen ihnen)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select curated_companies curated_esg_emissions"

# Schnell: alle Modelle im curated-Schema
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select curated"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select curated"
```

Erwartung Tests: **PASS=13/13**.

### Trusted

```bash
# Build
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select trusted_esg_emissions"

# Test (zeigt 6 erwartete FAILures auf not_null scope_1_tco2e —
# das ist gewollt; das externe Quality Gate ist die Pflicht-Pruefung)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select trusted_esg_emissions"
```

### Komplett-ESG-Pipeline auf einmal

```bash
# Alle ESG-Modelle bauen + testen (ohne Snapshots)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select stg_cdp_emissions stg_nzdpu_emissions curated_companies curated_esg_emissions trusted_esg_emissions"
```

---

## Selektor-Patterns mit Layer-Praefix

dbt erlaubt Layer-Auswahl ueber das `+`-Suffix (Downstream) bzw.
`+`-Praefix (Upstream).

```bash
# Alles ab einem Source bauen + testen (Source und alles abwaerts)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select source:raw.fondspreise+"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select source:raw.cdp_emissions+"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select source:raw.nzdpu_emissions+"

# Alles inkl. Trusted und seine Upstream-Abhaengigkeiten
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select +trusted_esg_emissions"

# Nur Tests fuer alles, was Curated upstream betrifft
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select +curated_esg_emissions"
```

---

## Diagnose-Befehle

```bash
# Welche Modelle/Snapshots sind enabled?
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --resource-type model"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --resource-type snapshot"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --resource-type test"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --resource-type source"

# Ressourcen entlang einer Lineage
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --select +trusted_esg_emissions"

# Compile-only (kein Run, kein Side-Effect)
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt compile --select curated_esg_emissions"

# Doku generieren + offline ablegen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt docs generate"
```

---

## State-Machine-Aufrufe (Hosts-Skripte, nutzen dbt intern)

```bash
# ESG-Pipeline auf einen Zielzustand bringen
./scripts/demo2-state.sh raw_cur          # Raw + Staging + Curated
./scripts/demo2-state.sh raw_trusted      # vollstaendig (ohne Quality Gate)
./scripts/demo2-state.sh empty            # alles droppen
./scripts/demo2-state-verify.sh           # Status pro Layer

# Demo-1-Reset (Tabellen droppen + Load 1 + Staging + Snapshot)
./scripts/reset-demo1.sh

# ESG-Promotion ueber Quality Gate (PowerShell)
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh

# ESG-Promotion ueber Quality Gate (Bash)
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

---

## Erwartete Test-Counts pro Selektor

| Selektor                                    | PASS-Anzahl | Bemerkung |
|---------------------------------------------|-------------|-----------|
| `source:raw.fondspreise+`                   | 19/19       | Source-Tests + Staging + Snapshot + Snapshot-Tests |
| `stg_fondspreise`                           | 6/6         | Staging-Tests |
| `snp_fondspreise_scd2`                      | 6/6         | Snapshot-Tests |
| `stg_nzdpu_emissions`                       | 8/8         | inkl. `accepted_values` auf scope_1_unit |
| `stg_cdp_emissions`                         | 6/6         | bewusst kein not_null auf isin / scope_1 |
| `curated_companies curated_esg_emissions`   | 13/13       | inkl. relationships, unique, unique_combination |
| `trusted_esg_emissions`                     | 7/8 (6 FAIL) | not_null scope_1_tco2e ist erwartet rot |

---

## Was du **nicht** ausfuehren solltest

- **`dbt seed`** — keine Seed-Files im Repo. Befehl wird zwar
  fehlerfrei durchlaufen (No-Op), aber ohne Effekt.
- **`dbt run`** **ohne Selektor** _vor_ dem ersten
  `./scripts/demo2-state.sh raw_cur`-Lauf — die Trusted-Tabelle
  haengt von Curated ab, das Curated-Modell haengt von Staging,
  Staging vom Raw-Layer. Ohne Raw-Daten produzieren die Models
  leere Outputs (kein Fehler, aber sinnlos).
- **`dbt run --full-refresh`** auf den ESG-Modellen — sie sind
  bereits `materialized='table'` (Vollersatz bei jedem `dbt run`),
  `--full-refresh` aendert hier nichts.
- **`dbt run --select stg_cdp_emissions stg_nzdpu_emissions`**
  als isoliertes Refresh _ohne_ vorherige Raw-Ingestion — die
  Models laufen, aber das Ergebnis ist nur so frisch wie der
  letzte Raw-Stand.
