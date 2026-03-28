# Architektur

## Komponentenuebersicht

| Container | Image | Aufgabe |
|-----------|-------|---------|
| `minio` | `minio/minio` | S3-kompatibler Objektspeicher — speichert Parquet-Dateien und Iceberg-Metadaten |
| `postgres` | `postgres:16` | Relationale Datenbank als persistentes Backend fuer Nessie |
| `nessie` | `projectnessie/nessie` | Iceberg REST Catalog — verwaltet Tabellen-Registrierung, Schema-Versionen, Snapshots |
| `trino` | `trinodb/trino` | Verteilte SQL-Engine — liest Iceberg-Tabellen direkt aus MinIO |
| `spark-master` + `spark-worker` | custom (Spark 3.5) | Spark-Cluster fuer Batch-Ingestion |
| `jupyter` | custom (pyspark-notebook) | Interaktive Notebook-Umgebung mit PySpark + Trino-Client |
| `minio-init` | `minio/mc` | Einmaliger Init-Container — legt S3-Buckets an |

---

## Datenfluss

```
Quelldateien (CSV / JSON)
        |
        v
  spark-submit (spark-master)
        |
        |-- schreibt Parquet-Dateien --> MinIO (s3://raw/<tabelle>/)
        |-- registriert Tabellen    --> Nessie (REST API)
        |
        v
  Nessie speichert Metadaten in PostgreSQL

  Trino / Jupyter
        |-- liest Tabellen-Metadaten von Nessie
        |-- liest Parquet-Dateien direkt von MinIO
```

Spark und Trino teilen keinen Compute-Layer. Sie kommunizieren nur ueber das gemeinsame Storage (MinIO) und den gemeinsamen Catalog (Nessie). Das ist "Separation of Storage and Compute".

---

## Layer-Architektur (Medallion)

| Layer | Bucket | Inhalt | Schreiben | Status |
|-------|--------|--------|-----------|--------|
| Raw | `s3://raw/` | Daten wie geliefert, minimale Typanpassung, append-only | Spark | verfuegbar |
| Staging | `s3://staging/` | Bereinigte, typisierte Daten — Fehler gefiltert, Nulls behandelt | dbt via Trino | AP-7 |
| Curated | `s3://curated/` | Fachlich aggregierte Daten, reportingfertig | dbt via Trino | AP-7 |

Raw-Tabellen werden nie ueberschrieben — nur appended. Historisierung laeuft ueber Iceberg-Snapshots.

---

## Iceberg-Tabellen im Raw Layer

| Tabelle | Partitionierung | Bemerkung |
|---------|----------------|-----------|
| `nzdpu_emissions` | keine | Nested JSON, 30 Unternehmen x 3 Jahre |
| `cdp_emissions` | keine | Alle Spalten als STRING (Bereinigung im Staging) |
| `owid_co2_countries` | `year` | 5 Partitionen (2020-2024) |
| `fund_master` | keine | Kleine Lookup-Tabelle |
| `fund_positions` | `position_date` | 2 Partitionen |

---

## Mapping Sandbox zu Produktion

Diese Sandbox spiegelt die geplante Architektur eines produktiven Lakehouse wider.
Die Konzepte sind identisch — nur die konkreten Produkte unterscheiden sich.

| Komponente | Sandbox | Produktion (Beispiel) |
|------------|---------|----------------------|
| Objektspeicher | MinIO | FI-TS S3 / AWS S3 |
| Iceberg Catalog | Nessie (REST) | Polaris / Nessie Enterprise |
| SQL-Engine | Trino | Trino / Athena / Snowflake |
| Verarbeitung | Spark (lokal) | Spark on Kubernetes / EMR |
| Notebooks | Jupyter (Docker) | JupyterHub / SageMaker |
| Transformationen | dbt Core (AP-7) | dbt Cloud / dbt Core |
| Orchestrierung | (geplant) | Airflow / Dagster |

### Warum Nessie statt Polaris in der Sandbox?

Nessie bietet eine eingebettete Web-UI die den Catalog-Zustand (Tabellen, Branches, Commits) direkt sichtbar macht. Das ist fuer Demo-Zwecke wertvoller als die schlanke Polaris-API.
Beide implementieren das Iceberg REST Catalog Protocol — ein Wechsel ist ohne Code-Aenderung in Spark oder Trino moeglich.

---

## Netzwerk

Alle Container laufen im internen Docker-Netzwerk `lakehouse-net`.
Container kommunizieren ueber ihre Service-Namen (z.B. `http://nessie:19120`).
Nur die in `.env` definierten Ports sind auf dem Host verfuegbar.
