# Demo 2 — Migration der ESG-Pipeline auf File-level Raw

Stand: 2026-04-26
Zweck: Uebergangs-Doku waehrend der ESG-Pipeline-Umstellung
(AP-7 bis AP-12). Zwischen AP-10 und AP-11/12 sind Teile des
dbt-Projekts bewusst deaktiviert.

## Warum sind ESG-Modelle temporaer deaktiviert?

Mit **AP-8/AP-9** wurden die Iceberg-Tabellen
`nessie.raw.cdp_emissions` und `nessie.raw.nzdpu_emissions` von der
alten Record-level Ingestion (strukturierte Spalten via Spark) auf
**File-level Raw** umgestellt:

- 1 Row pro geladener Datei
- `raw_payload` enthaelt den kompletten Datei-Inhalt als String
- Provenance-Spalten beschreiben den Ursprung
- Parsing der Records ist Staging-Aufgabe, nicht Raw-Aufgabe

Mit **AP-10** wurde `dbt/models/sources.yml` an dieses neue Schema
angepasst. Damit ist die Source-Definition korrekt — aber die
bestehenden Staging-Modelle in `dbt/models/staging/` referenzieren
noch die alten strukturierten Spalten (`isin`, `scope_1_tco2e`, ...),
die in der File-level Raw-Tabelle nicht mehr existieren.

Wuerden diese Modelle in diesem Zustand laufen, wuerde
`dbt run --select stg_cdp_emissions stg_nzdpu_emissions` mit
*Column not found* abbrechen.

Loesung: die drei betroffenen Modelle sind per `enabled=false`
**deaktiviert**, bis AP-11 sie umschreibt:

| Modell                      | Status     | Reaktivierung in |
|-----------------------------|------------|------------------|
| `stg_cdp_emissions`         | disabled   | AP-11b           |
| `stg_nzdpu_emissions`       | disabled   | AP-11a           |
| `curated_esg_emissions`     | disabled   | AP-12            |

Die SQL-Dateien selbst bleiben erhalten — nur ein `config`-Block
und ein Migrations-Kommentar wurden vorangestellt. AP-11/AP-12
ueberschreibt die Inhalte komplett.

## Welche APs reaktivieren die Modelle?

- **AP-11a**: `stg_nzdpu_emissions` neu — doppelter `UNNEST` ueber
  `data[]` und `reporting_periods[]`, expandiert auf 90 Rows
  (30 Companies x 3 Jahre).
- **AP-11b**: `stg_cdp_emissions` neu — `split(raw_payload, chr(10))`
  + `WITH ORDINALITY` + Spalten-Extraktion, expandiert auf 100 Rows.
- **AP-12**: `curated_esg_emissions` reaktiviert auf neuem Staging,
  inkl. UNION + Quality Gate.

## Was passiert wenn man trotzdem `dbt run --select staging+` aufruft?

dbt bemerkt `enabled=false` und ueberspringt die ESG-Staging-Modelle
geraeuschlos. `stg_fondspreise` wird normal gebaut (Demo 1 ist
unabhaengig). Erwartete Ausgabe:

```
Running with dbt=1.x
Found N models, ... sources, ...
...
Concurrency: 1 threads
1 of 1 START sql table model staging.stg_fondspreise [RUN]
1 of 1 OK created sql table model staging.stg_fondspreise [...]
Done. PASS=1 ...
```

`dbt list --resource-type model` zeigt:
- `stg_fondspreise`: enabled
- `stg_cdp_emissions`: disabled
- `stg_nzdpu_emissions`: disabled
- `curated_esg_emissions`: disabled

`dbt parse` muss fehlerfrei durchlaufen — Syntax wird auch fuer
deaktivierte Modelle geprueft, `ref()`-Beziehungen aber nicht
aufgeloest.

## Was passiert mit den alten staging/curated-Iceberg-Tabellen?

Frueher angelegte Tabellen wie `nessie.staging.stg_nzdpu_emissions`
oder `nessie.curated.curated_esg_emissions` koennen aus
vorhergehenden dbt-Laeufen noch existieren. AP-10 raeumt sie
**nicht** auf. AP-11/AP-12 ueberschreiben sie automatisch beim
ersten dbt-Lauf der neuen Modelle.

Falls in der Zwischenzeit jemand die alten Tabellen explizit
loswerden moechte: `DROP TABLE` direkt in Trino/Spark gegen Nessie.
Ein automatisierter Reset wird in AP-16 als Bestandteil des
Demo-2-Reset-Skripts geliefert.

## Verifikation

Nach AP-10 muss folgendes ohne Fehler laufen:

```bash
dbt parse
dbt list --resource-type model            # nur enabled
dbt build --select source:raw.fondspreise+
```

Erwartet:
- `dbt parse` keine Fehler (nur Pre-existing-Deprecation-Warnings)
- `dbt list --resource-type model` zeigt nur `stg_fondspreise`
  (deaktivierte Modelle erscheinen standardmaessig nicht). Im
  `target/manifest.json` stehen `stg_cdp_emissions`,
  `stg_nzdpu_emissions`, `curated_esg_emissions` unter dem
  Top-Level-Key `disabled`.
- `dbt build --select source:raw.fondspreise+` baut
  `stg_fondspreise` + Snapshot + Tests komplett gruen
  (Verifikation in AP-10: PASS=19/19).
