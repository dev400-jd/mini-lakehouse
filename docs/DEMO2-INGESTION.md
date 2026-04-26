# Demo 2 — Ingestion: Raw Layer CDP (Variante B)

Workshop 3 "Datenreifeprozess in Aktion", Demo 2 (CDP-Teil).
Zeigt File-level Raw + Quality Gate am Beispiel ESG-Emissionsdaten.

## Architektur-Entscheidung: Variante B — File-level Payload

Jede geladene CSV-Datei erzeugt **genau einen Iceberg-Row** in
`nessie.raw.cdp_emissions`. `raw_payload` enthaelt den kompletten
Dateiinhalt als UTF-8-String — byte-identisch zum Original
(inkl. Header-Zeile, Newlines, evtl. BOM).

Identisches Prinzip wie bei der Fondspreis-Pipeline (Demo 1, AP-2b),
nur mit CSV statt JSON. Der Unterschied: ein zusaetzliches Feld
`source_file_format = "csv"` macht das Format am Raw-Row explizit,
weil Staging-Modelle pro Format unterschiedlich parsen muessen
(JSON: `json_extract`, CSV: `split` + Spalten-Position).

**Warum nicht Record-level?** Re-Serialisierung via `csv.writer()`
veraendert Whitespace, Quoting und Line-Endings — der Hash waere
nicht stabil. File-level Payload ist die einzige Form, in der
`sha256(raw_payload.encode("utf-8")) == source_file_hash` gilt.

Das CSV-Parsing (Header lesen, Spalten extrahieren) ist Aufgabe
des Staging Layers (AP-11b).

---

## Ablaeuft fuer CDP

| Schritt | Skript | Was passiert |
|---------|--------|--------------|
| 0. Init | `init-cdp-table.py` | Alte Record-level Tabelle droppen, neue File-level Tabelle anlegen |
| 1. Ingest | `ingest-cdp.py --file ...` | 1 Row: komplette CSV-Datei als String |
| 2. Verify | `verify-cdp-ingestion.py` | Hash, Size, CSV-Parsebarkeit, Header pruefen |

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

## Ausfuehrung

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

## Tabellenstruktur

```sql
-- nessie.raw.cdp_emissions
-- Iceberg v2, Partitionierung: days(ingestion_timestamp)
-- Location: s3a://raw/cdp_emissions
-- 1 Row pro geladener CSV-Datei

ingestion_id           STRING    -- UUID pro Ingestion-Run
ingestion_timestamp    TIMESTAMP -- UTC, Zeitpunkt des Loads
source_system          STRING    -- konstant "cdp"
source_version         STRING    -- "v1" (Default; aus Dateinamen erweiterbar)
source_file_path       STRING    -- absoluter Pfad der gelesenen Datei
source_file_hash       STRING    -- "sha256:<hex>" — Integritaetscheck
source_file_size_bytes BIGINT    -- Plausibilitaetscheck: == len(raw_payload.encode())
source_file_format     STRING    -- "csv" — fuer Staging-Disambiguierung
raw_payload            STRING    -- KOMPLETTER CSV-Inhalt als UTF-8-String (byte-identisch)
```

Unterschied zum Schema von `raw.fondspreise`: zusaetzliche Spalte
`source_file_format`. CDP ist die erste Quelle, die das Feld nutzt;
`raw.fondspreise` bleibt unveraendert (die Tabelle weiss implizit,
dass ihr Format JSON ist).

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

## Staging-Vorgriff: CSV-Zeilen aus raw_payload extrahieren (Trino)

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

---

## Demo-Reset

Wiederholtes Init + Ingest + Verify ist idempotent: `init-cdp-table.py`
dropped + recreated, jeder Ingest-Lauf produziert einen neuen Snapshot.
Fuer einen sauberen Demo-Restart genuegt:

```powershell
docker compose exec spark-master spark-submit /scripts/init-cdp-table.py
docker compose exec spark-master spark-submit /scripts/ingest-cdp.py --file /data/sample/cdp_emissions.csv
docker compose exec spark-master spark-submit /scripts/verify-cdp-ingestion.py
```
