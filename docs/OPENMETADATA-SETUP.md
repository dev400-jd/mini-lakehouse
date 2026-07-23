# OpenMetadata Setup (DE2 — Metadaten-Katalog)

Stand: 2026-07-23, praktisch verifiziert (Compose-Aufbau, Migration,
Health-Check) auf nativem Linux, OpenMetadata-Server-Image `1.5.11`.

Zweck: Reproduzierbare Anleitung fuer das DE2-Teilprojekt
(`DE-2-Hackathon-Projekt.docx`) — OpenMetadata als operativer Katalog,
im Minimal-Zuschnitt neben dem bestehenden Mini-Lakehouse-Stack.

## Architektur-Kurzfassung

Eigenes Compose-File `docker-compose.openmetadata.yml`, per `-f`
zusaetzlich geladen — der Kern-Stack (`docker-compose.yml`) bleibt
unangetastet. Vier Services, die sich Netzwerk (`lakehouse-net`) und
Postgres mit dem Basis-Stack teilen:

| Service | Rolle | Lebensdauer |
|---|---|---|
| `openmetadata-db-init` | legt `openmetadata_db` im bestehenden Postgres an | einmalig |
| `elasticsearch` | Suchindex (Lineage-Graph, Suche, Glossar) | dauerhaft |
| `openmetadata-migrate` | Flyway-Schema-Migration + Suchindex-Mapping | einmalig |
| `openmetadata-server` | Anwendung + UI (Port 8585) | dauerhaft |

Start-Reihenfolge via `depends_on`: `db-init` → `elasticsearch` →
`migrate` → `server`. Kein eigenes MySQL/Postgres, kein
Airflow-Ingestion-Container (Ingestion laeuft separat per CLI, siehe
Briefing Abschnitt 6).

## Voraussetzungen

- Basis-Stack laeuft bereits (`docker compose up -d`), Postgres ist
  `healthy`.
- Genug RAM frei (siehe Briefing Abschnitt 4, Hebel 2: Spark/Jupyter
  waehrend OpenMetadata-Arbeit stoppen).

## Start

```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d openmetadata-server
```

Das startet transitiv die gesamte Kette. Erststart zieht zwei neue
Images (Elasticsearch, OpenMetadata-Server — mehrere hundert MB,
nicht auf schwachem WLAN einplanen, siehe Pre-Flight im Briefing).

## Verifikation

```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml \
  ps openmetadata-db-init elasticsearch openmetadata-migrate openmetadata-server
```

Erwartet: `openmetadata-db-init` und `openmetadata-migrate` `Exited (0)`,
`elasticsearch` und `openmetadata-server` `healthy`.

UI: `http://localhost:8585`, Login `admin` / `admin`.

## Troubleshooting — bereits aufgetretene Probleme

Zwei reale Probleme sind beim ersten Testlauf aufgetreten (genau die
"Ueberraschungen", die das Briefing fuer dieses ungetestete Setup
ankuendigt) und sind mit den obigen Compose-Definitionen bereits
behoben. Zur Einordnung, falls sie auf anderer Hardware/Version wieder
auftauchen:

**Server crasht in Restart-Schleife, Log zeigt
`relation "openmetadata_settings" does not exist"` /
`Make sure you have run './bootstrap/openmetadata-ops.sh migrate'`**

Das `openmetadata-server`-Image fuehrt die Flyway-Migration **nicht
automatisch** beim Start aus. Deshalb der eigene
`openmetadata-migrate`-Init-Container. Falls das trotzdem wieder
auftritt (z. B. nach manuellem `docker volume rm` der Postgres-Daten),
manuell nachholen:

```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml \
  run --rm --no-deps --entrypoint /opt/openmetadata/bootstrap/openmetadata-ops.sh \
  openmetadata-server migrate
```

Wichtig: **kein** `-c <pfad>` anhaengen — der Wrapper im Image setzt
den Pfad zur mitgelieferten `conf/openmetadata.yaml` bereits selbst
(relativ zu seinem eigenen Skript-Pfad). Ein zusaetzliches `-c` fuehrt
zu `option '--config' should be specified only once`.

**Healthcheck des Servers schlaegt dauerhaft fehl (`unhealthy`),
obwohl die UI unter Port 8585 antwortet**

Das Server-Image ist Alpine-basiert und enthaelt **kein `curl`**
(anders als die uebrigen Services in diesem Compose-File). Healthcheck
muss `wget -q --spider` statt `curl` verwenden — bereits so in
`docker-compose.openmetadata.yml` hinterlegt.

## Stoppen / Abbauen

Ohne den Basis-Stack anzufassen:

```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml \
  rm -sf openmetadata-server elasticsearch openmetadata-migrate openmetadata-db-init
```

## dbt-Lineage-Ingestion (Trino-Connector + dbt-Artefakte)

Stand: 2026-07-23, **ungetestet** (analog zum Rest dieses Files — vor
dem Hackathon-Tag einmal durchspielen). Ziel: die dbt-Modelle
(`staging`/`curated`/`trusted`, siehe `dbt/dbt_project.yml`) inkl.
Lineage-Graph und Testergebnissen in OpenMetadata sichtbar machen.

Kein Airflow-Ingestion-Container vorhanden (`PIPELINE_SERVICE_CLIENT_ENABLED:
"false"`, siehe Kommentar in `docker-compose.openmetadata.yml`) — Ingestion
laeuft per CLI im bestehenden `jupyter`-Container, der bereits Netzwerkzugriff
auf `trino` und `openmetadata-server` (gleiches `lakehouse-net`) sowie den
dbt-Projektordner (`/home/jovyan/dbt`) hat.

Zwei Workflow-YAMLs liegen unter `config/openmetadata/` (automatisch nach
`/home/jovyan/config` gemounted, siehe `docker-compose.yml`):

- `ingest-trino.yaml` — registriert `raw`/`staging`/`curated`/`trusted` aus
  dem `nessie`-Catalog als Database-Service `trino_lakehouse`.
- `ingest-dbt.yaml` — haengt Lineage, Column-Descriptions und
  Test-Ergebnisse aus den dbt-Artefakten an die per `ingest-trino.yaml`
  angelegten Tabellen.

**Reihenfolge ist zwingend**: `ingest-trino.yaml` zuerst, da die
dbt-Ingestion Lineage nur an bereits existierende Table-Entities haengt,
diese aber nicht selbst anlegt. Beide YAMLs nutzen `${OPENMETADATA_JWT_TOKEN}`
statt eines fest hinterlegten Secrets — Injektion per `envsubst`, gleiches
Muster wie `config/jupyter/before-spark-conf.sh`.

### 1. JWT-Token besorgen

UI (`http://localhost:8585`, Login `admin`/`admin`) → **Settings → Bots →
ingestion-bot** → Token generieren/kopieren, dann lokal exportieren:

```bash
export OPENMETADATA_JWT_TOKEN="<token aus der UI>"
```

### 2. Ingestion-Package installieren

Nicht Teil des `jupyter/Dockerfile` (wuerde jeden Rebuild verlangsamen,
wird nur fuer diesen einen Zweck gebraucht) — **und bewusst in einem
eigenen venv**, nicht im Haupt-Python des Containers:

```bash
docker compose exec jupyter bash -c \
  "python3 -m venv /home/jovyan/.venvs/om-ingest \
   && /home/jovyan/.venvs/om-ingest/bin/pip install --no-cache-dir 'openmetadata-ingestion[trino,dbt]==1.5.11'"
```

Zwei Gruende dafuer, beide verifiziert (2026-07-23):

- **Version muss zum Server passen.** `openmetadata-ingestion` ohne
  Versions-Pin zieht die neueste PyPI-Version (z.B. `1.13.1.1`), der
  Server-Image-Tag steht aber auf `OPENMETADATA_VERSION=1.5.11` in
  `.env`. Client/Server muessen in Major.Minor uebereinstimmen, sonst
  bricht `metadata ingest` sofort mit `Server version is 1.5.11 vs.
  Client version ...` ab.
- **Package-Konflikt mit dbt-trino im selben Environment.** Pinnt man
  `openmetadata-ingestion==1.5.11` direkt ins Haupt-`pip` des
  Containers (statt in ein venv), zieht dessen `dbt`-Extra
  `dbt-core==1.8.x`/altes `dbt-common`/`dbt-adapters` und ueber-
  schreibt die vom `jupyter/Dockerfile` installierte `dbt-trino`-Kette
  — danach fehlt der `trino`-Adapter in `dbt --version` und die
  eigentliche dbt-Pipeline ist kaputt. Installiert man `dbt-trino`
  hinterher neu, um dbt zu reparieren, zieht *das* wiederum ein neueres
  `sqlfluff`/`protobuf`, was `metadata` mit einem Import-Error
  (`sqlfluff.core.helpers.dict`) zerschiesst. Ein eigenes venv fuer
  `openmetadata-ingestion` umgeht den Konflikt komplett, weil beide
  Toolchains dann getrennte `site-packages` haben.

### 3. Trino-Tabellen registrieren

```bash
docker compose exec -e OPENMETADATA_JWT_TOKEN jupyter bash -c \
  'envsubst < /home/jovyan/config/openmetadata/ingest-trino.yaml > /tmp/ingest-trino.yaml \
   && /home/jovyan/.venvs/om-ingest/bin/metadata ingest -c /tmp/ingest-trino.yaml'
```

### 4. catalog.json erzeugen

`dbt/target/` enthaelt nach `dbt run`/`dbt build` `manifest.json` und
`run_results.json`, aber **kein** `catalog.json` — das entsteht erst bei:

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt docs generate"
```

Ohne `catalog.json` liefert die dbt-Ingestion Lineage, aber keine
Column-level-Details.

### 5. dbt-Artefakte einspielen

```bash
docker compose exec -e OPENMETADATA_JWT_TOKEN jupyter bash -c \
  'envsubst < /home/jovyan/config/openmetadata/ingest-dbt.yaml > /tmp/ingest-dbt.yaml \
   && /home/jovyan/.venvs/om-ingest/bin/metadata ingest -c /tmp/ingest-dbt.yaml'
```

### 6. Verifikation

UI → **Explore → trino_lakehouse** → eine Tabelle in `trusted` oeffnen →
Tab **Lineage** sollte `raw` → `staging` → `curated` → `trusted` zeigen,
Tab **dbt** die Testergebnisse aus `run_results.json`.

## Offene Punkte vor dem Hackathon-Tag

- **Image-Version `1.5.11`** und die verwendeten Env-Var-Namen
  (`DB_DRIVER_CLASS`, `OM_DATABASE`, `ELASTICSEARCH_HOST`, ...) sind
  der dokumentierte Weg, aber nicht gegen die aktuelle offizielle
  Release-Seite (get.openmetadata.org) verifiziert. Vor dem
  Pre-Flight-Abend gegenpruefen, falls eine neuere Version gewaehlt
  wird.
- Der Abschnitt "dbt-Lineage-Ingestion" ist **praktisch verifiziert**
  (2026-07-23): Trino-Ingestion 20 Records/0 Errors, dbt-Ingestion 168
  Records/0 Errors, beide bei 100% Success. Wichtig dabei: `metadata`
  muss aus dem in Schritt 2 angelegten venv
  (`/home/jovyan/.venvs/om-ingest/bin/metadata`) laufen, nicht aus dem
  Haupt-Python des Containers — siehe Begruendung in Schritt 2.
