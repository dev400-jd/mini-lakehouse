# Demo 2: Quality Gate mit Great Expectations

Workshop 3 "Datenreifeprozess in Aktion", Demo 2 — Quality-Gate-Teil.
Great Expectations prueft das Curated-Modell `curated_esg_emissions`,
bevor Daten in den Trusted Layer promotet werden. Wenn die Suite
rot ist, blockiert die Promotion (Orchestrierung in AP-14).

## Architektur

GE laeuft als eigenstaendiges Tool neben dbt:

```
dbt run --select curated   →   GE checkpoint   →   dbt run --select trusted
                                  ^ blockt Promotion bei FAIL
```

Connection: SQLAlchemy `trino://dbt@localhost:8080/nessie` (kein Auth
in der Sandbox). GE 0.18.x mit dem `trino`-SQLAlchemy-Dialect aus
dem offiziellen `trino`-Python-Client.

## Komponenten

| Pfad                                                                        | Zweck |
|-----------------------------------------------------------------------------|-------|
| `great_expectations/great_expectations.yml`                                 | Datasource gegen Trino, Stores, Data-Docs |
| `great_expectations/expectations/curated_esg_emissions_suite.json`          | 6 Expectations |
| `great_expectations/checkpoints/curated_esg_checkpoint.yml`                 | Checkpoint-Definition mit `InferredAssetSqlDataConnector` |
| `great_expectations/uncommitted/`                                           | Validation-Results + Data Docs (nicht committed) |

## Aufruf

Nach jedem `dbt run --select curated`:

**PowerShell:**

```powershell
# Variante 1: Python API (robust gegen Console-Encoding-Probleme unter Windows)
uv run python -c "import great_expectations as gx; r = gx.get_context(context_root_dir='great_expectations').run_checkpoint(checkpoint_name='curated_esg_checkpoint'); raise SystemExit(0 if r.success else 1)"

# Variante 2: GE CLI (PowerShell setzt PYTHONIOENCODING per $env: vor dem Aufruf)
$env:PYTHONIOENCODING = "utf-8"; uv run great_expectations checkpoint run curated_esg_checkpoint
```

**Git Bash / WSL:**

```bash
PYTHONIOENCODING=utf-8 uv run great_expectations checkpoint run curated_esg_checkpoint
```

Exit-Code 0 bei gruenem Gate, 1 bei mindestens einer roten Expectation.
AP-14 wird darauf basierend die Trusted-Promotion blockieren.

## Die 6 Expectations

| # | Expectation                              | Spalte / Argument                                   | Zweck |
|---|------------------------------------------|-----------------------------------------------------|-------|
| 1 | `expect_table_row_count_to_be_between`   | min=100, max=200                                    | Plausibilitaets-Bandbreite |
| 2 | `expect_column_values_to_not_be_null`    | isin                                                | NULL-ISINs sind in Curated bereits gefiltert |
| 3 | `expect_column_value_lengths_to_equal`   | isin = 12                                           | ISIN-Format-Plausibilitaet |
| 4 | `expect_column_values_to_be_in_set`      | source_system in {nzdpu, cdp}                       | Whitelist der erlaubten Quellen |
| 5 | `expect_column_values_to_be_between`     | scope_1_tco2e zwischen 0 und 100M, mostly=0.95      | Wertebereich + 5% Toleranz fuer Outlier |
| 6 | `expect_compound_columns_to_be_unique`   | (isin, reporting_year, source_system)               | Composite-Eindeutigkeit |

Aktueller Stand auf den Sample-Daten: **6/6 PASS**, 150 Rows.

## Data Docs

Nach `ctx.build_data_docs()` (oder `update_data_docs`-Action im
Checkpoint) ist der aktuelle Stand einsehbar unter:

```
great_expectations/uncommitted/data_docs/local_site/index.html
```

Im Browser per `file://` oeffnen. Diese HTML-Reports sind die
Evidenz-Form fuer Compliance-Reporting (DORA Art. 11 —
Datenintegritaet).

## Demo-Anwendung in Workshop 3

Die Suite wird in der Demo zweimal ausgefuehrt:

1. **Initial gruen** auf den sauberen Curated-Daten — Promotion in
   Trusted laeuft.
2. **Live rot gemacht**, z.B. via einem direkten INSERT in Trino,
   der ein Duplikat in `(isin, reporting_year, source_system)`
   erzeugt — `expect_compound_columns_to_be_unique` faellt rot, das
   Gate blockiert.

## Bekannte Einschraenkungen

- **Kein RuntimeDataConnector** in dieser Konfiguration: GE 0.18 kann
  Spalten aus einer ad-hoc Runtime-Query unter dem Trino-Dialect
  nicht zuverlaessig introspectieren (`columns()` liefert eine leere
  Liste, jede Expectation faellt mit "column does not exist"). Wir
  nutzen daher `InferredAssetSqlDataConnector` mit Schema-Filter auf
  `curated`. Der Connector ist trotzdem in der Datasource definiert
  und kann fuer andere Zwecke (z.B. ad-hoc CTE-Validierungen)
  verwendet werden.

- **Asset-Naming**: Asset-Namen sind unqualified Table-Namen
  (`curated_esg_emissions`), nicht `<schema>.<table>`. Der
  Schema-Filter in der Datasource sorgt fuer Eindeutigkeit.

- **Suite-Pflege**: Bei Schema-Aenderungen am Curated-Modell muss
  die Suite manuell angepasst werden. Spaltennamen sind hardcoded
  in der Suite-JSON.

- **Console-Encoding (Windows)**: Die GE-CLI gibt Unicode-Status-
  Symbole aus, die unter cp1252 (`charmap`-codec) zu
  `UnicodeEncodeError` fuehren. Workaround: `PYTHONIOENCODING=utf-8`
  setzen oder die Python-API verwenden (siehe oben).

## Verifikation (Stand AP-13)

```text
Validation success: True
Statistics: {'evaluated_expectations': 6, 'successful_expectations': 6,
             'unsuccessful_expectations': 0, 'success_percent': 100.0}
```

---

## Promotion-Skript (AP-14)

`scripts/promote-trusted-esg.py` orchestriert die Pipeline
**Curated -> Quality Gate -> Trusted** als drei klar getrennte
Phasen. Es ist gleichzeitig das Live-Material in Demo 2.

### Phasen

| Phase | Aktion                                        | Ausfuehrungsort         |
|-------|-----------------------------------------------|-------------------------|
| 1     | `dbt run --select curated`                    | jupyter-Container       |
| 2     | GE Checkpoint `curated_esg_checkpoint`        | lokale uv-Umgebung      |
| 3     | `dbt run --select trusted_esg_emissions`      | jupyter-Container       |

**Hinweis zur Ausfuehrung**: dbt laeuft in dieser Sandbox nicht
lokal, sondern im jupyter-Container. Das Skript ruft Phase 1 und 3
daher ueber `docker compose exec jupyter bash -lc "cd /home/jovyan/dbt && dbt ..."`
auf. GE laeuft lokal (uv-Venv).

### Aufruf

**PowerShell:**

```powershell
# Einmalig pro Session: Encoding fuer Python auf UTF-8 setzen
$env:PYTHONIOENCODING = "utf-8"

# Standard-Pfad (alle drei Phasen)
uv run python scripts/promote-trusted-esg.py

# Phase 1 ueberspringen (z.B. nach manueller Curated-Manipulation
# fuer den Demo-Roten-Pfad — sonst wuerde der Refresh die
# Manipulation ueberschreiben)
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

**Git Bash / WSL:**

```bash
# In ~/.bashrc empfohlen: export PYTHONIOENCODING=utf-8
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

### Exit-Codes

| Code | Bedeutung                                                                |
|------|--------------------------------------------------------------------------|
| 0    | Promotion erfolgreich — `trusted_esg_emissions` aktualisiert             |
| 1    | Gate rot — mindestens 1 Expectation FAIL, Trusted bleibt unveraendert    |
| 2    | Technischer Fehler (dbt-Fehlschlag, GE-Connection-Problem etc.)          |

### Demo-Choreografie

**Szenario A — Gruener Pfad (Standard):**

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py
```

Output zeigt:
- `PHASE 1` — Curated 150 Rows
- `PHASE 2` — 6/6 Expectations gruen, "GATE GRUEN -- Promotion freigegeben"
- `PHASE 3` — Trusted 150 Rows
- "PROMOTION ERFOLGREICH"

**Szenario B — Roter Pfad (Manipulation):**

In CloudBeaver/Trino vor dem Skript-Aufruf einen Verstoss in Curated
einfuegen (`expect_column_values_to_be_in_set` auf `source_system`
schlaegt zuverlaessig bei jedem unbekannten Wert fehl):

```sql
INSERT INTO nessie.curated.curated_esg_emissions
    (isin, reporting_year, source_system,
     scope_1_tco2e, scope_2_location_tco2e, scope_2_market_tco2e, scope_3_total_tco2e,
     verification, ingestion_id, ingestion_timestamp, source_file_hash)
VALUES
    ('DE0007236101', 2099, 'manipulation',
     DECIMAL '5000000000.000', NULL, NULL, NULL,
     'manual', '00000000-0000-0000-0000-000000000000',
     TIMESTAMP '2026-04-27 12:00:00', 'sha256:demo');
```

Skript mit `--skip-curated-refresh` aufrufen, damit der Curated-
Rebuild die Manipulation nicht wieder ueberschreibt:

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

Output zeigt:
- Phase 1 uebersprungen
- `PHASE 2` — 5/6 OK, 1/6 FAIL (`expect_column_values_to_be_in_set(source_system)`)
- "GATE ROT -- Promotion blockiert"
- Verweis auf Data Docs HTML
- Phase 3 wird **nicht** ausgefuehrt
- Exit 1

Trusted ist **unveraendert** (= 150 Rows aus dem letzten gruenen Lauf,
kein Manipulations-Row).

**Aufraeumen nach der Demo:**

```sql
DELETE FROM nessie.curated.curated_esg_emissions
WHERE source_system = 'manipulation';
```

Anschliessend laeuft der Standard-Pfad wieder gruen durch.

### Troubleshooting

- **`UnicodeEncodeError` beim Skript-Aufruf**: Windows-Terminal
  setzt cp1252 als stdout-Encoding. Workaround:
  - PowerShell: `$env:PYTHONIOENCODING = "utf-8"` vor dem Aufruf
    (gilt fuer die aktuelle Session; persistent ueber
    `[Environment]::SetEnvironmentVariable("PYTHONIOENCODING","utf-8","User")`).
  - Git Bash / WSL: `export PYTHONIOENCODING=utf-8` in `~/.bashrc`.
  - **Bash-Inline-Form (`PYTHONIOENCODING=utf-8 uv run ...`) funktioniert
    in PowerShell NICHT** — PowerShell interpretiert die Form als
    Befehlsname und scheitert mit `CommandNotFoundException`.
  Das Skript setzt den UTF-8-Wrapper zusaetzlich selbst, falls die
  Variable nicht gesetzt ist.
- **`docker compose exec: not running`**: Stack starten mit
  `docker compose up -d`.
- **GE-Checkpoint-Exception ("table does not exist")**: Curated-
  Modelle erst per `dbt run --select curated` (oder durch das Skript
  selbst ohne `--skip-curated-refresh`) bauen.

### Verifikation (Stand AP-14)

| Test                                           | Erwartung           | Ergebnis |
|------------------------------------------------|---------------------|----------|
| Standard-Lauf, kein Manipulation               | Exit 0, Trusted=150 | OK       |
| Mit Manipulation `source_system='manipulation'`| Exit 1, Trusted=150 | OK       |
| Trusted-Counts vor/nach rotem Lauf             | unveraendert        | OK       |
