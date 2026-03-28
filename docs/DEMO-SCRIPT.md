# Demo-Skript

Zwei Varianten: 30-Minuten-Kurzversion fuer fokussierte Demos, 60-Minuten-Langversion fuer tiefere Einfuehrungen.

---

## Vorbereitung (vor dem Termin)

- [ ] `docker compose up -d` ausgefuehrt, alle 7 Services `healthy`
- [ ] `make seed` ausgefuehrt, 5 Tabellen geladen
- [ ] Jupyter geoeffnet: http://localhost:8888?token=lakehouse
- [ ] MinIO Console geoeffnet: http://localhost:9001
- [ ] Nessie UI geoeffnet: http://localhost:19120
- [ ] CloudBeaver geoeffnet: http://localhost:8978 (Admin-Passwort einmalig setzen)
- [ ] Kernel in Notebook 01 gestartet (erste Zelle ausgefuehrt)
- [ ] RAM-Auslastung geprueft — Docker sollte >= 8 GB frei haben

Wichtig: `make seed` vor jeder Demo neu ausfuehren wenn Notebook 02 bereits gelaufen ist.

---

## 30-Minuten-Kurzversion

### Einleitung (3 Min)

Was wir zeigen: Wie ein modernes Lakehouse funktioniert.
Drei Kernpunkte:
- Offenes Tabellenformat (Iceberg) statt proprietaerer Formate
- Storage und Compute sind getrennt — verschiedene Engines lesen dieselben Daten
- Versionierung eingebaut — kein manuelles Backup

### Notebook 01 — Iceberg erkunden (12 Min)

Jupyter oeffnen → `01_iceberg_erkunden.ipynb`

1. `SHOW TABLES IN nessie.raw` — fuenf Tabellen, alle per Spark geschrieben
2. Data Files — Parquet-Dateien auf MinIO, Spark kennt jede einzelne
3. Manifest Files — Iceberg-Index mit Min/Max-Statistiken pro Datei
4. Snapshots — jeder Schreibvorgang als versionierter Snapshot
5. Tabellen-Vergleich — unterschiedliche Groessen, Partitionierungen, Dateianzahlen

**Wow-Moment**: Parquet-Dateipfade in der Tabelle zeigen → "Die liegen direkt auf S3, jede Engine kann sie lesen."

### Notebook 02 — Schema Evolution & Time Travel (12 Min)

Jupyter → `02_time_travel_schema_evolution.ipynb`

1. JSON-Vergleich V1 vs. V2 — gleiche Daten, komplett andere Struktur
2. Spark flattened V2 live — `col("entity.name").alias("company_name")`
3. `ALTER TABLE ADD COLUMNS` — reine Metadaten-Operation, kein Daten-Rewrite
4. Alte Zeilen haben NULL fuer neue Felder — korrekt, kein Fehler
5. `VERSION AS OF <snapshot_id>` — Zustand vor der Aenderung in einer Zeile
6. Trino-Report laeuft unveraendert weiter

**Wow-Moment**: Time Travel — "Hier ist der exakte Zustand der Tabelle von vor 5 Minuten."

### Abschluss (3 Min)

Was als naechstes kommt: dbt fuer Raw → Staging → Curated Transformationen.

---

## 60-Minuten-Langversion

### Einleitung (5 Min)

Architektur-Diagramm zeigen (README.md). Jede Komponente kurz erklaeren.
Frage: Warum Iceberg statt "Parquet in S3"?

### MinIO Console (5 Min)

http://localhost:9001 → Bucket `raw`

- Ordnerstruktur zeigen: `fund_positions/data/position_date=2023-12-31/` → Partitionierung sichtbar
- `fund_positions/metadata/` → Iceberg-Metadaten: `.json`, `.avro` Dateien
- Zeigen dass Parquet-Dateien direkt downloadbar sind — kein Lock-in

### Nessie UI (5 Min)

http://localhost:19120

- Branch `main` → alle 5 Tabellen
- Eine Tabelle aufklappen → Commit-Historie
- Erlaeutern: Nessie ist der Katalog, nicht der Speicher. Die Daten liegen auf MinIO.

### Notebook 01 — Iceberg erkunden (15 Min)

Alle Zellen durchlaufen (s. Kurzversion, aber mehr Zeit fuer Fragen).

Zusatz: `DESCRIBE EXTENDED` zeigen — Iceberg kennt Location, Format-Version, Snapshot-ID.

### Notebook 02 — Schema Evolution & Time Travel (15 Min)

Alle Zellen durchlaufen. Besondere Betonung:

- Beim Flattening erklaeren warum Mapping sinnvoller ist als Tabelle umbenennen
- Schema Evolution: "Kein ALTER TABLE REBUILD, kein Downtime"
- Bonus-Zelle: `RENAME COLUMN` — Parquet-Dateien haben interne Feld-IDs, keine Namen

### CloudBeaver — SQL-Editor im Browser (5 Min)

http://localhost:8978 → Verbindung "Lakehouse (Trino)" ist bereits vorkonfiguriert.

SQL Editor oeffnen:

```sql
SELECT sector, count(*), round(avg(scope_1_tco2e)) AS avg_scope1
FROM nessie.raw.nzdpu_emissions
GROUP BY sector
ORDER BY avg_scope1 DESC;
```

Pointe: Keine CLI, kein lokales Tool — SQL direkt im Browser auf den Iceberg-Tabellen.

### Trino CLI (alternativ, 5 Min)

```bash
docker compose exec trino trino
```

```sql
SHOW CATALOGS;
USE nessie.raw;
SHOW TABLES;
SELECT sector, count(*), round(avg(scope_1_tco2e)) FROM nzdpu_emissions GROUP BY 1;
```

Pointe: Trino hat nie von Spark gehoert. Es liest direkt von MinIO ueber den Nessie-Catalog.

### dbt-Platzhalter (5 Min)

Erklaeren was als naechstes kommt:
- dbt-Modelle definieren Raw → Staging → Curated Transformationen in SQL
- Staging: Typen korrigieren, Nulls behandeln, CDP-Drecksdaten bereinigen
- Curated: ESG-Scores berechnen, Portfolio-Risiko aggregieren

### Abschluss & Fragen (5 Min)

---

## Wo sind die Wow-Momente?

| Moment | Zelle | Warum wirkungsvoll |
|--------|-------|--------------------|
| Parquet-Pfade auf MinIO | Notebook 01, Data Files | Macht "offenes Format" greifbar |
| Manifest-Statistiken | Notebook 01, Manifests | Erklaert warum Iceberg schneller als Hive ist |
| V1 vs. V2 JSON nebeneinander | Notebook 02, JSON-Vergleich | Zeigt reales Problem, kein konstruiertes Beispiel |
| `VERSION AS OF` | Notebook 02, Time Travel | Time Travel in einer Zeile SQL |
| Report laeuft unveraendert | Notebook 02, Trino-Report | Beweist rueckwaertskompatible Schema Evolution |

## Haeufige Fragen

**"Warum nicht einfach Delta Lake?"**
Delta Lake ist Apache-lizenziert, Databricks hat aber proprietaere Erweiterungen. Iceberg ist komplett vendor-neutral und wird von AWS, Google, Apple, Netflix gepflegt.

**"Kann Trino auch schreiben?"**
Ja, aber in dieser Demo schreibt nur Spark. Trino-Writes sind moeglich, haben aber andere Charakteristiken (kein native Streaming).

**"Wie gross koennen die Tabellen werden?"**
Iceberg ist produktiv bei Petabyte-Groessen im Einsatz (Netflix, Apple). Die Sandbox laeuft mit Spielzeugdaten, das Konzept skaliert.

**"Was ist mit Sicherheit / Zugriffsrechten?"**
In der Sandbox offen. In Produktion: Nessie/Polaris unterstuetzen RBAC auf Katalog-Ebene, MinIO S3-Bucket-Policies auf Storage-Ebene.
