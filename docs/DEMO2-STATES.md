# Demo 2 — State-Machine fuer Pipeline-Zustaende

`scripts/demo2-state.sh` bringt die ESG-Pipeline in einen
definierten Zielzustand. **Idempotent**: jeder Aufruf droppt zuerst
alle Demo-2-Tabellen und baut dann den Zielzustand komplett neu auf.

Demo-1-Pipeline (Fondspreise) bleibt unberuehrt.

## Verfuegbare Zustaende

| State          | Raw | Staging | Curated | Trusted | Erwartete Counts                                                     |
|----------------|-----|---------|---------|---------|----------------------------------------------------------------------|
| `empty`        |     |         |         |         | alle 7 Demo-2-Tabellen leer / nicht vorhanden                        |
| `raw`          | OK  |         |         |         | raw.cdp 1 / raw.nzdpu 1                                              |
| `raw_stg`      | OK  | OK      |         |         | + stg_cdp 100 / stg_nzdpu 90                                         |
| `raw_cur`      | OK  | OK      | OK      |         | + curated_companies 30 / curated_esg_emissions 150                   |
| `raw_trusted`  | OK  | OK      | OK      | OK      | + trusted_esg_emissions 150 (ohne Quality-Gate-Schritt)              |

## Aufruf

```bash
./scripts/demo2-state.sh <state>
```

Aufruf ohne Argument oder mit ungueltigem State: Usage-Meldung auf
stderr, Exit 1. Bei nicht-laufendem Stack: Exit 2.

## Verifikation

```bash
./scripts/demo2-state-verify.sh
```

Gibt fuer jede der 7 Demo-2-Tabellen die aktuelle Zeilenanzahl aus.
Nicht-existente Tabellen erscheinen als `— rows`.

Beispiel-Output nach `raw_cur`:

```
Demo-2-Pipeline Status:
  raw.cdp_emissions                             1 rows
  raw.nzdpu_emissions                           1 rows
  staging.stg_cdp_emissions                     100 rows
  staging.stg_nzdpu_emissions                   90 rows
  curated.curated_companies                     30 rows
  curated.curated_esg_emissions                 150 rows
  trusted.trusted_esg_emissions                 — rows
```

## Demo-Empfehlungen

- **Vor Demo 2**: `./scripts/demo2-state.sh raw_cur`
  Damit ist Curated befuellt, Trusted leer. In der Live-Demo rufst
  du dann `scripts/promote-trusted-esg.py` (siehe AP-14) fuer die
  Quality-Gate-Demonstration auf — das Gate fuegt sich nahtlos an.

- **Layer-Drilldown**: `./scripts/demo2-state.sh raw` oder `raw_stg`,
  falls eine bestimmte Layer-Station isoliert gezeigt werden soll.

- **Vollstaendiger Neustart**: `./scripts/demo2-state.sh empty`
  gefolgt vom gewuenschten Zielzustand.

- **Snapshot pruefen** (z.B. fuer Time-Travel-Demo): nach `raw`
  die Snapshot-Metadaten der Raw-Tabellen abfragen.

## Beziehung zum Quality-Gate-Skript (AP-14)

Das State-Machine-Skript baut Trusted **direkt** via `dbt run` —
ohne Quality-Gate-Pruefung. Das ist bewusst: die State-Machine ist
ein Setup-Werkzeug, kein Promotion-Werkzeug.

Fuer die Promotion-Demo:

1. `./scripts/demo2-state.sh raw_cur` — Curated bauen, Trusted leer
2. `uv run python scripts/promote-trusted-esg.py` (PowerShell:
   `$env:PYTHONIOENCODING = "utf-8"` voranstellen) — Quality Gate
   pruefen, bei gruenem Gate Trusted promoten.

## Beziehung zur Demo-1-Pipeline

Die State-Machine beruehrt **ausschliesslich** Demo-2-Tabellen
(`raw.cdp_emissions`, `raw.nzdpu_emissions`, `staging.stg_*_emissions`,
`curated.curated_*`, `trusted.trusted_esg_emissions`).

Demo-1-Tabellen (`raw.fondspreise`, `staging.stg_fondspreise`,
`curated.snp_fondspreise_scd2`) bleiben in jedem State unberuehrt.

## Zeit-Budget (Richtwerte)

| State          | Laufzeit |
|----------------|----------|
| `empty`        | ca. 20s  |
| `raw`          | 60-90s   |
| `raw_stg`      | 90-120s  |
| `raw_cur`      | 100-130s |
| `raw_trusted`  | 130-160s |

Gemessen auf Standard-Demo-Hardware (Win11, Docker Desktop, 16GB RAM).

## Implementations-Hinweise

- **Drop-Reihenfolge**: umgekehrte Dependency-Reihenfolge —
  Trusted -> Curated -> Staging -> Raw. Aenderungen an dieser
  Reihenfolge koennen zu Foreign-Key-aehnlichen Problemen fuehren.
- **Deterministische Timestamps**: Ingestion verwendet feste
  Timestamps (`2026-04-20T08:15:00Z` fuer CDP,
  `2026-04-20T08:30:00Z` fuer NZDPU) — damit Iceberg-Snapshots
  reproduzierbar sind und die Folien-Inhalte stimmen.
- **PySpark via spark-master**: Init- und Ingest-Skripte werden
  via `docker compose exec spark-master spark-submit ...`
  aufgerufen, nicht ueber lokales `uv run` — die Skripte brauchen
  den Spark/Iceberg-Klassenpfad des Spark-Masters.
- **dbt via jupyter**: `dbt run`-Aufrufe gehen via
  `docker compose exec jupyter bash -c "cd /home/jovyan/dbt && ..."` —
  dbt ist nur im jupyter-Container installiert.
- **Schemas werden NICHT angelegt**: das State-Machine-Skript setzt
  voraus, dass die Schemas (raw, staging, curated, trusted) bereits
  existieren. Falls nicht: einmalig `scripts/init-schemas.sh`
  ausfuehren oder Stack komplett neu starten.
