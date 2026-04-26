# Demo 2 — Ingestion: Raw Layer CDP + NZDPU (Variante B)

Workshop 3 "Datenreifeprozess in Aktion", Demo 2.
Zeigt File-level Raw + Quality Gate am Beispiel ESG-Emissionsdaten
aus zwei Quellen: CDP (CSV) und NZDPU (JSON).

## Architektur-Entscheidung: Variante B — File-level Payload

Jede geladene Datei erzeugt **genau einen Iceberg-Row** in der
zugehoerigen Raw-Tabelle (`nessie.raw.cdp_emissions` bzw.
`nessie.raw.nzdpu_emissions`). `raw_payload` enthaelt den kompletten
Dateiinhalt als UTF-8-String — byte-identisch zum Original
(inkl. Header-Zeile / Wrapper-Whitespace, Newlines, evtl. BOM).

Identisches Prinzip wie bei der Fondspreis-Pipeline (Demo 1, AP-2b).
Der Unterschied: das zusaetzliche Feld `source_file_format`
(`"csv"` oder `"json"`) macht das Format am Raw-Row explizit, weil
Staging-Modelle pro Format unterschiedlich parsen muessen
(JSON: `json_extract`, CSV: `split` + Spalten-Position).

**Warum nicht Record-level?** Re-Serialisierung via `csv.writer()`
oder `json.dumps()` veraendert Whitespace, Quoting, Key-Reihenfolge
und Line-Endings — der Hash waere nicht stabil. File-level Payload
ist die einzige Form, in der
`sha256(raw_payload.encode("utf-8")) == source_file_hash` gilt.

Das strukturierte Parsing (CSV-Header, JSON-Array-Expand) ist
Aufgabe des Staging Layers (AP-11b ff.).

---

## Ablaeufe pro Quelle

### CDP (CSV)

| Schritt | Skript | Was passiert |
|---------|--------|--------------|
| 0. Init | `init-cdp-table.py` | Alte Record-level Tabelle droppen, neue File-level Tabelle anlegen |
| 1. Ingest | `ingest-cdp.py --file ...` | 1 Row: komplette CSV-Datei als String |
| 2. Verify | `verify-cdp-ingestion.py` | Hash, Size, CSV-Parsebarkeit, Header pruefen |

### NZDPU (JSON)

| Schritt | Skript | Was passiert |
|---------|--------|--------------|
| 0. Init | `init-nzdpu-table.py` | Alte Record-level Tabelle droppen, neue File-level Tabelle anlegen |
| 1. Ingest | `ingest-nzdpu.py --file ...` | 1 Row: komplette JSON-Datei als String |
| 2. Verify | `verify-nzdpu-ingestion.py` | Hash, Size, Wrapper-Schluessel, 30 Companies × 3 Periods |

> **Hinweis:** `scripts/spark-ingestion.py` und
> `scripts/spark-ingestion-v2.py` (alte Record-level Ingestion) bleiben
> im Repo, werden aber **nicht mehr ausgefuehrt**. Sie wuerden die
> neue Tabelle in den alten Stil zurueckbauen. AP-9 ersetzt sie
> vollstaendig durch `ingest-nzdpu.py`.

---

## Voraussetzungen

```powershell
docker compose up -d
docker compose ps
```

Quelldatei `data/sample/cdp_emissions.csv` liegt im Repo (siehe
[ESG-DATA-INVENTORY.md](ESG-DATA-INVENTORY.md)) — 14 973 Bytes,
100 Datenzeilen + 1 Header, 15 Spalten.

---

## Ausfuehrung CDP

### 0. Init (einmalig pro Demo-Run)

```powershell
docker compose exec spark-master spark-submit /scripts/init-cdp-table.py
```

Erwartete Ausgabe (sinngemaess):
```
  Tabelle nessie.raw.cdp_emissions gedropped (alte Record-level Daten verworfen).
  Tabelle nessie.raw.cdp_emissions angelegt (File-level Schema, Iceberg v2).
  Spalten: 9
```

Beim allerersten Lauf (Tabelle existierte noch nicht) erscheint
stattdessen `existierte nicht — neu anlegen.`

### 1. Ingest

```powershell
docker compose exec spark-master spark-submit /scripts/ingest-cdp.py --file /data/sample/cdp_emissions.csv --ingestion-timestamp 2026-04-20T08:30:00Z
```

Erwartete Ausgabe:
```
  Reading: /data/sample/cdp_emissions.csv
  File size: 14,973 bytes
  File hash: sha256:8cd0e509378fb27ebb451261ef204098...
  Source system: cdp
  Source version: v1
  Source format: csv

  Ingestion ID: <uuid>
  Ingestion timestamp: 2026-04-20T08:30:00Z

  Writing to Iceberg: nessie.raw.cdp_emissions
  Records written: 1 (file-level)

  Current snapshot count for raw.cdp_emissions: 1
```

### 2. Verify

```powershell
docker compose exec spark-master spark-submit /scripts/verify-cdp-ingestion.py
```

Pro Row gepruefte Bedingungen:
- `source_file_format == "csv"`
- `sha256(raw_payload.encode("utf-8")) == source_file_hash`
- `len(raw_payload.encode("utf-8")) == source_file_size_bytes`
- `csv.reader(StringIO(raw_payload))` liefert 101 Zeilen
- Header-Zeile entspricht dem erwarteten 15-Spalten-Header
- 100 Datenzeilen

---

## Ausfuehrung NZDPU

### 0. Init (einmalig pro Demo-Run)

```powershell
docker compose exec spark-master spark-submit /scripts/init-nzdpu-table.py
```

### 1. Ingest

```powershell
docker compose exec spark-master spark-submit /scripts/ingest-nzdpu.py --file /data/sample/nzdpu_emissions.json --ingestion-timestamp 2026-04-20T08:30:00Z
```

Erwartete Ausgabe:
```
  Reading: /data/sample/nzdpu_emissions.json
  File size: 75,491 bytes
  File hash: sha256:4cc86529bc23073a57f4ffb820ef49a7...
  Source system: nzdpu_fallback
  Source version: v1
  Source format: json

  Ingestion ID: <uuid>
  Ingestion timestamp: 2026-04-20T08:30:00Z

  Writing to Iceberg: nessie.raw.nzdpu_emissions
  Records written: 1 (file-level)

  Current snapshot count for raw.nzdpu_emissions: 1
```

`source_system` wird aus dem JSON-Wrapper-Feld `source` gelesen
(`"nzdpu_fallback"` in der Sample-Datei). `source_version` ist nicht
im Wrapper enthalten und wird per `--source-version` gesetzt
(Default `"v1"`). Falls in Zukunft eine v2-Datei mit anderem Schema
ingested wird, kann der Wert manuell ueberschrieben werden.

### 2. Verify

```powershell
docker compose exec spark-master spark-submit /scripts/verify-nzdpu-ingestion.py
```

Pro Row gepruefte Bedingungen:
- `source_file_format == "json"`
- `sha256(raw_payload.encode("utf-8")) == source_file_hash`
- `len(raw_payload.encode("utf-8")) == source_file_size_bytes`
- `json.loads(raw_payload)` liefert Wrapper-Objekt mit
  `{status, total_records, source, data}`
- `len(parsed["data"]) == 30`
- `parsed["data"][0]["reporting_periods"]` hat 3 Eintraege
- Summe aller `reporting_periods` ueber alle 30 Companies == 90

---

## Tabellenstruktur

Beide Tabellen (`raw.cdp_emissions`, `raw.nzdpu_emissions`) haben
das identische File-level Schema:

```sql
-- nessie.raw.cdp_emissions   (s3a://raw/cdp_emissions)
-- nessie.raw.nzdpu_emissions (s3a://raw/nzdpu_emissions)
-- Iceberg v2, Partitionierung: days(ingestion_timestamp)
-- 1 Row pro geladener Datei

ingestion_id           STRING    -- UUID pro Ingestion-Run
ingestion_timestamp    TIMESTAMP -- UTC, Zeitpunkt des Loads
source_system          STRING    -- "cdp" / "nzdpu_fallback" (aus JSON-Wrapper)
source_version         STRING    -- "v1" (Default; --source-version override)
source_file_path       STRING    -- absoluter Pfad der gelesenen Datei
source_file_hash       STRING    -- "sha256:<hex>" — Integritaetscheck
source_file_size_bytes BIGINT    -- Plausibilitaetscheck: == len(raw_payload.encode())
source_file_format     STRING    -- "csv" / "json" — fuer Staging-Disambiguierung
raw_payload            STRING    -- KOMPLETTER Dateiinhalt als UTF-8-String (byte-identisch)
```

Unterschied zum Schema von `raw.fondspreise`: zusaetzliche Spalte
`source_file_format`. CDP und NZDPU nutzen das Feld; `raw.fondspreise`
bleibt unveraendert (die Tabelle weiss implizit, dass ihr Format JSON
ist).

---

## Hash-Verifikation: warum byte-identisch?

```python
raw_bytes  = source_path.read_bytes()                  # 1. Bytes lesen
file_hash  = hashlib.sha256(raw_bytes).hexdigest()     # 2. Hash berechnen
raw_payload = raw_bytes.decode("utf-8")                # 3. Als String halten
# raw_payload.encode("utf-8") == raw_bytes (UTF-8 round-trip ist verlustfrei)
```

UTF-8-Dekodierung + erneute UTF-8-Kodierung ist verlustfrei — der
String wird zur Speicherung in Iceberg verwendet, der Hash bleibt
gegen die Originalbytes vergleichbar. Voraussetzung: die Datei ist
UTF-8 (BOM ist ok und bleibt erhalten). Bei nicht-UTF-8-Dateien
bricht das Skript mit `ERROR: File is not valid UTF-8` ab — wir
spekulieren nicht ueber Encodings.

`csv.reader` darf in der Ingest-Pfad-Logik **nicht** vorkommen — es
wuerde Felder de-/re-quoten, Whitespace normalisieren und Line-Endings
veraendern. Die Verifikation in `verify-cdp-ingestion.py` benutzt
`csv.reader` nur als Lese-Plausibilitaet auf dem bereits gespeicherten
String und schreibt nichts zurueck.

---

## Staging-Vorgriff: Records aus raw_payload extrahieren (Trino)

### CDP — split auf Newline

Vorbereitung fuer AP-11b. Die Query expandiert `raw_payload` per
`split` an `\n` und nummeriert die Zeilen mit `WITH ORDINALITY`:

```sql
-- File-level Raw: CSV-Zeilen aus raw_payload extrahieren
-- (vereinfachtes Pattern, finale Logik kommt in stg_cdp_emissions.sql)
SELECT
    r.ingestion_id,
    r.ingestion_timestamp,
    sequence_index AS row_number,
    line
FROM nessie.raw.cdp_emissions r
CROSS JOIN UNNEST(
    split(r.raw_payload, chr(10))
) WITH ORDINALITY AS u(line, sequence_index)
WHERE sequence_index > 1               -- Header-Zeile ueberspringen
  AND length(trim(line)) > 0           -- evtl. trailing newline ignorieren
ORDER BY r.ingestion_timestamp, sequence_index;
```

Diese Query ist die Basis fuer `stg_cdp_emissions.sql` in AP-11b.
Die finale Staging-Logik wird zusaetzlich:
- die einzelnen Spalten aus `line` extrahieren (vermutlich per
  `regexp_extract_all` oder Trino-spezifischem CSV-Parser),
- Datentypen casten (Year zu INT, Scope-Werte zu DOUBLE),
- die File-level Provenance (`ingestion_id`, `source_file_hash`)
  pro Datenzeile mitfuehren.

### NZDPU — doppelter UNNEST

NZDPU hat einen Wrapper mit `data[]` (Companies) und pro Company
ein eingebettetes `reporting_periods[]` (Jahre). Der Staging-Layer
braucht beide Ebenen flach pro (Company, Jahr) — das ergibt einen
doppelten `UNNEST`:

```sql
-- File-level Raw NZDPU: doppelter UNNEST
-- (vereinfachtes Pattern, finale Logik kommt in stg_nzdpu_emissions.sql)
SELECT
    r.ingestion_id,
    r.ingestion_timestamp,
    json_extract_scalar(company, '$.company_name')               AS company_name,
    json_extract_scalar(company, '$.isin')                       AS isin,
    json_extract_scalar(period,  '$.reporting_year')             AS reporting_year,
    CAST(json_extract_scalar(period, '$.scope_1.value') AS DOUBLE) AS scope_1_tco2e
FROM nessie.raw.nzdpu_emissions r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.data') AS ARRAY(JSON))
) AS u1(company)
CROSS JOIN UNNEST(
    CAST(json_extract(company, '$.reporting_periods') AS ARRAY(JSON))
) AS u2(period)
ORDER BY company_name, reporting_year;
```

Erwartetes Ergebnis: **90 Rows** (30 Companies x 3 Jahre).
Diese Query wird in einem spaeteren AP zur Basis fuer
`stg_nzdpu_emissions.sql`.

---

## Demo-Reset

Wiederholtes Init + Ingest + Verify ist idempotent: das jeweilige
`init-*.py` dropped + recreated, jeder Ingest-Lauf produziert einen
neuen Snapshot. Fuer einen sauberen Demo-Restart beider Quellen:

```powershell
# CDP
docker compose exec spark-master spark-submit /scripts/init-cdp-table.py
docker compose exec spark-master spark-submit /scripts/ingest-cdp.py --file /data/sample/cdp_emissions.csv
docker compose exec spark-master spark-submit /scripts/verify-cdp-ingestion.py

# NZDPU
docker compose exec spark-master spark-submit /scripts/init-nzdpu-table.py
docker compose exec spark-master spark-submit /scripts/ingest-nzdpu.py --file /data/sample/nzdpu_emissions.json
docker compose exec spark-master spark-submit /scripts/verify-nzdpu-ingestion.py
```

`data/sample/nzdpu_emissions_v2.json` wird **nicht** ueber diese
Pipeline geladen — sie gehoert zu WS1/WS2 und wird in
`02_time_travel_schema_evolution.ipynb` separat verwendet
(siehe [ESG-DATA-INVENTORY.md](ESG-DATA-INVENTORY.md)).
