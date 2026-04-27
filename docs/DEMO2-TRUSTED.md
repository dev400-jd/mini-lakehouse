# Demo 2 — Trusted Layer

Workshop 3 "Datenreifeprozess in Aktion", Demo 2 — Trusted-Teil.
Trusted ist das fachlich freigegebene Endprodukt der Pipeline.
In Demo 2 enthaelt Trusted ESG-Emissionsdaten, die das Quality
Gate (Great Expectations, AP-13) passiert haben.

## Was leistet Trusted?

- Single Source of Truth fuer Reporting & nachgelagerte Konsumenten.
- Garantie ueber strenge Spaltenchecks (s. unten).
- Provenance bleibt durchgereicht — jede Trusted-Zeile traegt
  `ingestion_id`, `ingestion_timestamp`, `source_file_hash`.

## Verhaeltnis zu Curated

| Aspekt                    | Curated                                              | Trusted                                              |
|---------------------------|------------------------------------------------------|------------------------------------------------------|
| Inhalt                    | UNION beider Quellen, ISIN-NULLs gefiltert            | 1:1 Pass-through aus Curated                          |
| `scope_1_tco2e` darf NULL? | Ja (CDP hat 6 Records ohne Scope 1)                 | **Nein** (`not_null`-Test im Schema)                 |
| Eingang                   | Direkt aus Staging                                   | Nur, wenn Quality Gate (GE) gruen ist                |
| Test-Schaerfe             | Smoke-/Lineage-Tests                                 | Vertragstests fuer Konsumenten                       |
| Materialisierung          | `nessie.curated.curated_esg_emissions`               | `nessie.trusted.trusted_esg_emissions`               |

Aktuell ist Trusted ein einfacher SELECT auf Curated. Strengere
fachliche Filter (z.B. nur `verification = 'third_party_verified'`)
koennen spaeter hier eingebaut werden — ohne Curated zu beruehren.

## Strenge Tests in Trusted

Im `models/trusted/schema.yml` laufen u.a.:

- `not_null` auf **`isin`**, **`reporting_year`**, **`source_system`**,
  **`scope_1_tco2e`**, `ingestion_id`, `ingestion_timestamp`.
- `accepted_values` auf `source_system` (nzdpu / cdp).
- `dbt_utils.unique_combination_of_columns` auf
  `(isin, reporting_year, source_system)`.

Die Schaerfung gegenueber Curated ist **nicht** technisch — sie ist
ein **Vertrag**: "Wer Trusted konsumiert, darf sich auf diese
Eigenschaften verlassen."

## Erwartetes Test-Verhalten in der Demo

Wenn Trusted ohne Quality Gate aus Curated befuellt wird, **wird
der Test `not_null_trusted_esg_emissions_scope_1_tco2e` rot** —
genau die 6 CDP-Records ohne Scope 1 fallen durch (von urspruenglich
11 NULL-`scope_1`-Records im Staging haben 5 zusaetzlich eine
NULL-ISIN und werden bereits durch den ISIN-Filter in Curated
entfernt).

Das ist **gewollt und didaktisch zentral**:

1. Praesentator zeigt: Trusted ist gebaut, aber `dbt test` ist rot.
2. Frage ans Publikum: "Wer fängt das?"
3. Antwort: das Quality Gate (AP-14). Erst wenn GE gruen ist,
   wird Trusted promotet — und dann ist `not_null` automatisch
   gegeben, weil Curated bereits stimmig ist (oder die Promotion
   blockiert wurde).

> Die Versuchung, im Modell direkt `WHERE scope_1_tco2e IS NOT NULL`
> einzubauen, ist gross. **Bewusst nicht gemacht.** Ein verstecktes
> WHERE im SQL macht das externe Gate redundant.

## Schema-Initialisierung

Das Trusted-Schema wird beim Container-Start durch
`scripts/init-schemas.sh` (Service `trino-init`) angelegt:

```bash
CREATE SCHEMA IF NOT EXISTS nessie.trusted WITH (location = 's3a://trusted/')
```

Der zugehoerige Bucket `trusted` wird durch `scripts/init-buckets.sh`
(Service `minio-init`) erzeugt. Beide Skripte sind idempotent.

Manueller Setup (z.B. beim ersten Lauf in einer bereits laufenden
Sandbox, ohne Restart):

```powershell
# Bucket
docker compose exec minio mc mb --ignore-existing local/trusted

# Schema in Nessie
docker compose exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS nessie.trusted WITH (location = 's3a://trusted/')"
```

## Aufruf

Erst nach gruenem Quality Gate (GE checkpoint, siehe
`docs/DEMO2-QUALITY-GATE.md`):

```powershell
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select trusted_esg_emissions"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select trusted_esg_emissions"
```

Erwartung Stand AP-15:
- `dbt run` baut die Tabelle (150 Rows).
- `dbt test` ist **teilweise rot**: `not_null_..._scope_1_tco2e`
  schlaegt fehl mit 6 Failures. Das ist die Demo-Vorbereitung
  fuer AP-14.

## Verifikation

```sql
-- Counts
SELECT COUNT(*) AS rows_total
FROM nessie.trusted.trusted_esg_emissions;
-- erwartet: 150

-- Erwartete Test-Failures
SELECT COUNT(*) AS scope1_nulls
FROM nessie.trusted.trusted_esg_emissions
WHERE scope_1_tco2e IS NULL;
-- erwartet: 6
```
