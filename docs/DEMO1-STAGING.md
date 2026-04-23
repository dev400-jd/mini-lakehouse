# Demo 1 — Staging: stg_fondspreise

## Was macht stg_fondspreise?

Das Modell expandiert den File-level Raw Layer (`nessie.raw.fondspreise`) in
einzelne Fonds-Tag-Records — ein Row pro ISIN pro Handelstag.

**Transformation:**
- `CROSS JOIN UNNEST(...)` expandiert das `records[]`-Array aus `raw_payload`
- `business_date` wird von STRING nach DATE gecasted
- `nav` wird von STRING nach DOUBLE gecasted
- Provenance-Spalten (`ingestion_id`, `ingestion_timestamp`, `source_file_hash` etc.)
  werden unverändert durchgereicht

**Nicht hier:** Deduplizierung bei mehrfachen Loads desselben Stichtags — das
ist Aufgabe von Curated (AP-4b). Nach Load 1 + Load 2 existieren 2 Rows für
`DE000A1JX0V2, 2026-04-20` mit unterschiedlichem NAV.

---

## Ausführung

```powershell
docker compose exec jupyter dbt run --select stg_fondspreise --project-dir /home/jovyan/dbt --profiles-dir /home/jovyan/dbt
```

```powershell
docker compose exec jupyter dbt test --select stg_fondspreise --project-dir /home/jovyan/dbt --profiles-dir /home/jovyan/dbt
```

ESG-Modelle prüfen (Regression):
```powershell
docker compose exec jupyter dbt run --select stg_nzdpu_emissions --project-dir /home/jovyan/dbt --profiles-dir /home/jovyan/dbt
```

---

## Erwartete Row-Zahlen

| Zustand | Rows in stg_fondspreise |
|---------|------------------------|
| Nach Load 1 | 450 (5 Fonds × 90 Handelstage) |
| Nach Load 1 + Load 2 | 451 (450 + 1 Korrektur-Record) |

---

## Tests

| Test | Spalte | Erwartet |
|------|--------|---------|
| not_null | ingestion_id, isin, business_date, nav, currency | gruen |
| accepted_values ['EUR','USD'] | currency | gruen (Testdaten: 4× EUR, 1× USD) |

Kein `unique`-Test auf `(isin, business_date)` — nach Load 2 existieren
absichtlich 2 Rows für denselben Stichtag (Demo-Punkt für Time Travel / SCD2).

---

## Verifikations-Queries (Trino / CloudBeaver)

```sql
SELECT COUNT(*) FROM nessie.staging.stg_fondspreise;
```

```sql
SELECT isin, business_date, nav, source_version FROM nessie.staging.stg_fondspreise WHERE isin = 'DE000A1JX0V2' AND business_date = DATE '2026-04-20' ORDER BY ingestion_timestamp;
```
