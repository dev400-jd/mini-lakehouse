# Demo 1 — Ingestion: Raw Layer Fondspreise (Variante B)

Workshop 3 "Datenreifeprozess in Aktion", Demo 1.  
Zeigt Iceberg Time Travel und SCD2 am Beispiel Fondspreis-Zeitreihen.

## Architektur-Entscheidung: Variante B — File-level Payload

Jede geladene Datei erzeugt **genau einen Iceberg-Row**. `raw_payload` enthält
den kompletten Dateiinhalt als UTF-8-String — byte-identisch zum Original.

**Warum nicht Record-level (Variante A)?**  
Nur mit file-level Payload ist byte-identische Rekonstruktion möglich und
`sha256(raw_payload.encode("utf-8")) == source_file_hash` verifizierbar.
Bei Record-level-Re-Serialisierung via `json.dumps()` wäre Whitespace/
Key-Reihenfolge nicht garantiert — der Hash würde nicht stimmen.

Die JSON-Array-Expansion (`records[]` → einzelne Zeilen) ist Aufgabe
des Staging Layers, nicht des Raw Layers.

---

## Überblick

| Schritt | Was passiert |
|---------|-------------|
| 0. Drop | Alte Tabelle droppen (Schema-Migration) |
| 1. Load 1 | 1 Row: komplettes Load-1-JSON, Snapshot 1 |
| 2. Load 2 | 1 Row: komplettes Korrektur-JSON, Snapshot 2 |
| 3. Verify | 2 Rows, 2 Runs, 2 Snapshots, Hash-Verifikation |

Die Tabellen-Initialisierung ist **inline im Ingestion-Script** (`CREATE TABLE IF NOT EXISTS`).

---

## Voraussetzungen

```powershell
docker compose up -d
docker compose ps   # alle Services: Up (healthy)
```

Benötigte Dateien (aus AP-1, bereits vorhanden):
- `data/sample/fondspreise_load1.json`
- `data/sample/fondspreise_load2_correction.json`

---

## Schema-Migration: alte Tabelle droppen

Falls `nessie.raw.fondspreise` noch mit dem AP-2-Schema (451 Rows, record-level)
existiert, zuerst droppen:

```powershell
docker compose exec spark-master spark-submit /scripts/drop-fondspreise-table.py
```

Idempotent — kein Fehler wenn Tabelle nicht existiert.

---

## Ausführung

### Load 1 — Initialer Load

```powershell
docker compose exec spark-master spark-submit /scripts/ingest-fondspreise.py --file /data/sample/fondspreise_load1.json --ingestion-timestamp 2026-04-20T08:15:00Z
```

Erwartete Ausgabe:
```
  Reading: /data/sample/fondspreise_load1.json
  File size: ... bytes
  File hash: sha256:15075f5b4fa90cd79a3e06c6ededa51fdd9c3b5e57784092f8aa475a12b3c17f
  Parsed source_system:  fondsdaten_provider_xyz
  Parsed source_version: v1

  Ingestion ID:        <uuid>
  Ingestion timestamp: 2026-04-20T08:15:00Z

  Writing to Iceberg: nessie.raw.fondspreise
  Records written: 1 (file-level)

  Current snapshot count for raw.fondspreise: 1
```

### Load 2 — Korrektur-Load

```powershell
docker compose exec spark-master spark-submit /scripts/ingest-fondspreise.py --file /data/sample/fondspreise_load2_correction.json --ingestion-timestamp 2026-04-22T14:37:00Z
```

---

## Verifikation

```powershell
docker compose exec spark-master spark-submit /scripts/verify-fondspreise-ingestion.py
```

Prüft je Row:
- `sha256(raw_payload.encode("utf-8")) == source_file_hash`
- `len(raw_payload.encode("utf-8")) == source_file_size_bytes`
- `json.loads(raw_payload)["records"]` hat die erwartete Anzahl Elemente

---

## Tabellenstruktur

```sql
-- nessie.raw.fondspreise
-- Iceberg v2, Partitionierung: days(ingestion_timestamp)
-- Location: s3a://raw/fondspreise
-- 1 Row pro geladener Datei

ingestion_id           STRING    -- UUID pro Ingestion-Run
ingestion_timestamp    TIMESTAMP -- UTC, Zeitpunkt des Loads
source_system          STRING    -- aus JSON-Envelope: "fondsdaten_provider_xyz"
source_version         STRING    -- aus JSON-Envelope: "v1" oder "v1-correction"
source_file_path       STRING    -- absoluter Pfad der gelesenen Datei
source_file_hash       STRING    -- "sha256:<hex>" — Integritaetscheck
source_file_size_bytes BIGINT    -- Plausibilitaetscheck: muss == len(raw_payload.encode())
raw_payload            STRING    -- KOMPLETTER Dateiinhalt als UTF-8-String (byte-identisch)
```

---

## Staging-Expansion: Records aus raw_payload lesen (Trino)

Der Raw Layer liefert das Original-JSON als String. Im Staging Layer
wird der JSON-Array expandiert. Beispiel-Query in Trino / CloudBeaver:

```sql
-- File-level Raw (Variante B): JSON-Array expandieren
SELECT
    r.ingestion_id,
    r.ingestion_timestamp,
    json_extract_scalar(record_json, '$.isin')                    AS isin,
    json_extract_scalar(record_json, '$.fund_name')               AS fund_name,
    json_extract_scalar(record_json, '$.business_date')           AS business_date,
    CAST(json_extract_scalar(record_json, '$.nav') AS DOUBLE)     AS nav,
    json_extract_scalar(record_json, '$.currency')                AS currency
FROM nessie.raw.fondspreise r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.records') AS ARRAY(JSON))
) AS u(record_json)
ORDER BY r.ingestion_timestamp, isin, business_date;
```

Diese Query wird in Demo 1 live gezeigt um den Unterschied Raw → Staging
zu demonstrieren.

---

## Demo-Reset (Platzhalter — vollständig in AP-5)

```powershell
# Tabelle droppen
docker compose exec spark-master spark-submit /scripts/drop-fondspreise-table.py

# Danach beide Loads erneut ausführen (s.o.)
```

> AP-5 implementiert den vollständigen Demo-Reset als eigenes Script inkl.
> S3-Datei-Bereinigung.
