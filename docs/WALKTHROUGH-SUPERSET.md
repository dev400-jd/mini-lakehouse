# Walkthrough -- Apache Superset

> Ergänzung zur **WALKTHROUGH.md**. Dieses Dokument beschreibt
> ausschließlich die Installation, Konfiguration und Nutzung von Apache
> Superset im Mini-Lakehouse. Die allgemeine Inbetriebnahme des Stacks
> ist in der WALKTHROUGH.md dokumentiert.

------------------------------------------------------------------------

# Teil A -- Installation & Konfiguration

## Station 1: Architektur

**Kernbotschaft:** Superset ist ausschließlich Visualisierungs- und
Analysewerkzeug. Alle Datenzugriffe erfolgen über Trino.

``` text
Browser
   ↓
Apache Superset
   ↓ SQLAlchemy (sqlalchemy-trino)
Trino
   ↓
Nessie
   ↓
MinIO / Iceberg
```

Superset besitzt keine eigene Datenhaltung. Lediglich Metadaten wie
Benutzer, Dashboards, Datasets und Charts werden in PostgreSQL
gespeichert.

------------------------------------------------------------------------

## Station 2: Docker-Image

**Kernbotschaft:** Die Superset-Version wird ausschließlich in `.env`
gesteuert.

`.env`

``` text
SUPERSET_VERSION=6.1.0
```

`superset/Dockerfile`

``` dockerfile
ARG SUPERSET_VERSION

FROM apache/superset:${SUPERSET_VERSION}

USER root

RUN uv pip install \
    --python /app/.venv/bin/python \
    --no-cache \
    psycopg2-binary \
    sqlalchemy-trino \
    redis

USER superset
```

Dadurch existiert genau eine Stelle zur Pflege der Version.

------------------------------------------------------------------------

## Station 3: Infrastruktur

Zusätzliche Container:

  Service         Aufgabe
  --------------- -----------------
  redis           Cache & Celery
  superset-init   Initialisierung
  superset        Webserver

Metadatenbank:

``` text
postgresql://lakehouse:lakehouse123@postgres:5432/superset
```

------------------------------------------------------------------------

## Station 4: Initialisierung

``` bash
docker compose build --no-cache superset superset-init
docker compose up superset-init
docker compose up -d superset
```

Healthcheck

``` bash
curl http://localhost:8088/health
```

Erwartet:

``` text
OK
```

------------------------------------------------------------------------

# Teil B -- Betrieb

## Station 5: Trino anbinden

Neue Datenbank anlegen:

  Feld             Wert
  ---------------- ------------------------------------
  Display Name     Trino Lakehouse
  SQLAlchemy URI   trino://superset@trino:8080/nessie

Anschließend **Test Connection** und **Connect**.

------------------------------------------------------------------------

## Station 6: SQL Lab

``` sql
SHOW SCHEMAS IN nessie;

SHOW TABLES IN nessie.trusted;

SELECT *
FROM nessie.trusted.trusted_esg_emissions
LIMIT 100;
```

Erst wenn diese Abfragen funktionieren, sollte mit Charts begonnen
werden.

------------------------------------------------------------------------

## Station 7: Dataset

Data → Datasets → + Dataset

-   Database: Trino Lakehouse
-   Schema auswählen
-   Tabelle auswählen
-   Dataset erzeugen

Ein Dataset ist lediglich die semantische Beschreibung einer Tabelle.

------------------------------------------------------------------------

## Station 8: Erster Chart

Create Chart

-   Table
-   gewünschte Spalten
-   Update Chart
-   Save

Danach kann der Chart einem Dashboard hinzugefügt werden.

------------------------------------------------------------------------

# Teil C -- Troubleshooting

  ---------------------------------------------------------------------------------------
  Symptom                   Ursache                   Lösung
  ------------------------- ------------------------- -----------------------------------
  Login erreichbar, aber    Trino-Verbindung fehlt    Datenbankverbindung prüfen
  keine Daten                                         

  Keine Tabellen sichtbar   falscher Katalog/Schema   SHOW SCHEMAS / SHOW TABLES in Trino

  Verbindungstest schlägt   URI fehlerhaft            SQLAlchemy URI prüfen
  fehl                                                

  Änderungen am Dockerfile  altes Image               `docker compose build --no-cache`

  Änderungen an             Image veraltet            Superset neu bauen
  Python-Paketen                                      
  ---------------------------------------------------------------------------------------

------------------------------------------------------------------------

# Teil D -- Referenz

## Ports

  Dienst       URL
  ------------ -----------------------
  Superset     http://localhost:8088
  Trino        http://localhost:8080
  PostgreSQL   postgres:5432
  Redis        redis:6379

## Build

``` bash
docker compose build --no-cache superset superset-init
docker compose up superset-init
docker compose up -d superset
```

## Logs

``` bash
docker compose logs -f superset
docker compose logs -f superset-init
```

## Neustart

``` bash
docker compose restart superset
```
