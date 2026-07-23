# Mini-Lakehouse

Docker-basierte Sandbox, die die Kernkonzepte eines modernen Lakehouse demonstriert:
Apache Iceberg als offenes Tabellenformat, Nessie als versionierter Katalog, Spark zum Schreiben, Trino zum Abfragen — alles auf MinIO als S3-kompatiblem Objektspeicher.

---

## Schnellstart

```bash
git clone https://github.com/dev400-jd/mini-lakehouse.git
cd mini-lakehouse

docker compose up -d        # alle 7 Services starten
make seed                   # Beispieldaten laden (ca. 3 Min)
```

**Windows (ohne make):**

```powershell
docker compose up -d
bash scripts/seed-data.sh
```

Danach im Browser:

| Was | URL |
|-----|-----|
| Jupyter (Notebooks) | http://localhost:8888?token=lakehouse |
| CloudBeaver (SQL-Editor) | http://localhost:8978 |
| MinIO Console | http://localhost:9001 |
| Nessie UI | http://localhost:19120 |
| Trino Web UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8081 |

---

## Services & Ports

| Service | Port(s) | Beschreibung |
|---------|---------|--------------|
| MinIO API | 9000 | S3-kompatibler Objektspeicher |
| MinIO Console | 9001 | Web-UI: Buckets, Objekte, Pfade |
| PostgreSQL | 5432 | Metastore-Backend fuer Nessie |
| Nessie | 19120 | Iceberg REST Catalog mit Branch-Uebersicht |
| Trino | 8080 | Verteilte SQL-Engine, Web-UI |
| Spark Master | 7077 / 8081 | Spark-Cluster (7077 intern, 8081 Web-UI) |
| Jupyter | 8888 | Notebook-Umgebung (Token: `lakehouse`) |
| CloudBeaver | 8978 | Web-basierter SQL-Editor fuer Trino |

Alle Ports und Credentials sind in `.env` konfigurierbar. Standard: Benutzer `lakehouse`, Passwort `lakehouse123`.

---

## Architektur

```mermaid
flowchart LR
    subgraph Quellen
        J[JSON\nnzdpu_emissions]
        C[CSV\ncdp / owid / fonds]
    end

    subgraph Verarbeitung
        Spark[Apache Spark\nIngestion]
    end

    subgraph Katalog
        Nessie[(Nessie\nIceberg REST Catalog)]
        PG[(PostgreSQL\nMetadata-Backend)]
    end

    subgraph Speicher
        MinIO[(MinIO\nS3 - Parquet + Iceberg-Metadaten)]
    end

    subgraph Abfrage
        Trino[Trino\nSQL-Engine]
        Jupyter[Jupyter\nSparkSession + Trino]
    end

    J --> Spark
    C --> Spark
    Spark -->|schreibt Parquet| MinIO
    Spark <-->|registriert Tabellen| Nessie
    Nessie --- PG
    Trino <-->|liest Metadaten| Nessie
    Trino -->|liest Parquet| MinIO
    Jupyter -->|SparkSession local| MinIO
    Jupyter -->|JDBC| Trino
    Jupyter <-->|Catalog| Nessie
    CloudBeaver[CloudBeaver\nSQL-Editor] -->|JDBC| Trino
```

---

## Notebooks

| Notebook | Inhalt |
|----------|--------|
| `01_iceberg_erkunden.ipynb` | Anatomie einer Iceberg-Tabelle: Data Files, Manifest Files, Snapshots, Partitionen |

Das Notebook setzt `make seed` voraus. Time Travel und Snapshot-Historie lassen sich
per Trino auf `nessie.raw.fondspreise` zeigen (zwei Snapshots nach Demo-1-Load-2, siehe
[docs/DEMO1-DREHBUCH.md](docs/DEMO1-DREHBUCH.md)).

---

## Beispieldaten

`make seed` laedt sechs Tabellen in den Raw Layer (`s3://raw/`):

| Tabelle | Format | Zeilen | Beschreibung |
|---------|--------|--------|--------------|
| `nzdpu_emissions` | JSON, nested | 1 (File-level) | CO2-Emissionen (Scope 1-3) von 30 europaeischen Unternehmen, 3 Jahre — komplette JSON-Datei als `raw_payload`, wird in dbt-Staging entpackt |
| `cdp_emissions` | CSV | 1 (File-level) | CDP Climate Change Questionnaire — komplette CSV-Datei als `raw_payload`, wird in dbt-Staging entpackt |
| `fondspreise` | JSON | 1 (File-level) | Fondspreise Load 1 — komplette JSON-Datei als `raw_payload`, wird in dbt-Staging entpackt |
| `owid_co2_countries` | CSV | 100 | CO2 pro Land und Jahr, partitioniert nach `year` |
| `fund_master` | CSV | 10 | Fondsstammdaten mit ISINs |
| `fund_positions` | CSV | 319 | Fondspositionen, partitioniert nach `position_date` |

`nzdpu_emissions`, `cdp_emissions` und `fondspreise` liegen als File-level Raw vor
(eine Quelldatei = ein Iceberg-Row mit `raw_payload`), damit die dbt-Staging-Modelle
sie deterministisch entpacken. `owid_co2_countries`, `fund_master` und `fund_positions`
liegen geparst vor. `make seed` laedt Fondspreise Load 1 (Startzustand: 1 Row, 1 Snapshot);
Load 2 kommt live via [docs/DEMO1-DREHBUCH.md](docs/DEMO1-DREHBUCH.md).

Datengenerierung (Fallback-Daten sind bereits im Repository enthalten):

```bash
uv run scripts/generate-sample-data.py
```

---

## Voraussetzungen

- **Docker Desktop** mit mindestens 12 GB RAM
  - Windows: WSL2-Backend aktivieren und `.wslconfig` anpassen (siehe [docs/SETUP.md](docs/SETUP.md))
- **git**
- **make** — optional, auf Windows nicht standardmaessig vorhanden (Alternativen siehe Schnellstart)
- **uv** — nur fuer `scripts/generate-sample-data.py`, optional

---

## Konfiguration

Alle Versionen, Ports und Credentials stehen in `.env` (Single Source of Truth).
Docker Compose und alle Skripte lesen ausschliesslich aus dieser Datei.

---

## Weiterfuehrendes

- [docs/SETUP.md](docs/SETUP.md) — Installation, WSL2-Konfiguration, Troubleshooting
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — Komponentenuebersicht, Mapping Sandbox zu Produktion
- [docs/DEMO-SCRIPT.md](docs/DEMO-SCRIPT.md) — Gefuehrtes Demo-Skript (30 Min / 60 Min)
