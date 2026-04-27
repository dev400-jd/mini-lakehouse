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

```powershell
# Variante 1: Python API (robust gegen Console-Encoding-Probleme unter Windows)
uv run python -c "import great_expectations as gx; r = gx.get_context(context_root_dir='great_expectations').run_checkpoint(checkpoint_name='curated_esg_checkpoint'); raise SystemExit(0 if r.success else 1)"

# Variante 2: GE CLI (kann unter Windows mit cp1252-Console an Unicode-Symbolen scheitern)
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
