# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Docker-Compose "mini lakehouse" sandbox demonstrating a modern lakehouse architecture: Apache Iceberg tables on MinIO (S3-compatible), Project Nessie as a Git-like Iceberg catalog, Spark for ingestion, Trino for federated SQL, dbt for transformations, and Great Expectations as a data-quality gate. It's a demo/learning stack built for a hackathon (`docs/DEMO-SCRIPT.md`, `docs/DEMO1-*.md`, `docs/DEMO2-*.md`), not production — but it deliberately mirrors production concepts 1:1 (see "Sandbox → production mapping" below).

All configuration (image versions, ports, credentials) lives in `.env`, which is the single source of truth for `docker-compose.yml` and the `Makefile`. Default credentials throughout: `lakehouse` / `lakehouse123`.

## Common commands

### Stack lifecycle
```bash
docker compose up -d       # or: make up
make seed                  # loads the 6 raw-layer demo tables (~3 min): NZDPU, CDP, fondspreise, OWID, fund_master, fund_positions
make status / make logs / make down / make restart / make clean (down -v --remove-orphans)
make health                # scripts/healthcheck.sh — per-service health check
make pull                  # pre-pull all `image:`-based services — does NOT pull/build spark or jupyter base images (see Known gaps)
```
Windows without `make`: `bash scripts/seed-data.sh` directly.

### dbt — always runs inside the `jupyter` container, never on the host
```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run"                       # models only
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test"                       # tests only
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build"                      # run + test + snapshots
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_cdp_emissions"     # single model
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select stg_cdp_emissions"    # single model's tests
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select +trusted_esg_emissions"   # a model + upstream deps
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt build --select source:raw.fondspreise+"  # a source + everything downstream
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt list --select +trusted_esg_emissions"    # inspect lineage without running
```
`make dbt-run` / `make dbt-test` / `make dbt-docs` are shortcuts but assume `uv` is available in the `dbt/` dir on the host — the container-exec form above is the reliable pattern used throughout `docs/DBT-COMMANDS.md`. Full selector/test-count reference: `docs/DBT-COMMANDS.md`. Do not run `dbt seed` (no seed files, silent no-op) or `--full-refresh` on the ESG models (already `materialized='table'`, full-refresh changes nothing).

### Quality gate (Curated → Trusted promotion)
```bash
uv run python scripts/promote-trusted-esg.py                       # dbt curated refresh -> GE checkpoint -> dbt trusted (only if green)
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```
Exit codes: `0` promoted, `1` gate blocked (red expectation, Trusted untouched), `2` technical failure. This script — not `dbt test` — is the actual promotion gate; see Architecture below for why `dbt test` on `trusted_esg_emissions` is *expected* to show failures outside this flow.

### Demo state machines (idempotent: drop then rebuild to target state)
```bash
./scripts/demo2-state.sh {empty|raw|raw_stg|raw_cur|raw_trusted}   # ESG pipeline
./scripts/demo2-state-verify.sh                                    # status per layer
./scripts/reset-demo1.sh                                           # Fondspreise pipeline back to Load 1 + staging + snapshot
```

### DE1 subproject — NYC taxi engine comparison (Spark vs DuckDB vs Polars)
```bash
make fetch-taxi N=3                    # downloads N months of NYC TLC yellow-taxi parquet into data/taxi/ (gitignored, not committed), starting 2024-01
make fetch-taxi N=195 FROM=2009-01     # optional FROM=YYYY-MM overrides the start month (data exists back to 2009-01)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /scripts/taxi-landing-spark.py --year 2024 --month 1     # reference/baseline landing -> nessie.raw.taxi_spark
```
`config/duckdb/attach-nessie.sql` and `scripts/pyiceberg_init.py` are the connection recipes for the DuckDB and Polars/pyiceberg landing paths respectively (both run against Nessie's Iceberg-REST endpoint, mounted read-only into `jupyter` at `/home/jovyan/config` and `/home/jovyan/scripts`). Both files document required non-default flags — see "Known gaps / gotchas" below before touching this path.

### DE2 subproject — OpenMetadata operational catalog
Deliberately a *separate* Compose file, not merged into `docker-compose.yml`, so the core stack stays untouched and this piece is independently startable/stoppable — see the header comment in `docker-compose.openmetadata.yml` for the rationale. It shares `lakehouse-net` and the base stack's Postgres (own DB `openmetadata_db`, created by the one-shot `openmetadata-db-init` service via `scripts/init-openmetadata-db.sh`).
```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d openmetadata-server   # transitively starts db-init -> elasticsearch -> migrate -> server
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml \
  rm -sf openmetadata-server elasticsearch openmetadata-db-init openmetadata-migrate                 # tear down without touching the core stack
```
No Airflow ingestion container (`PIPELINE_SERVICE_CLIENT_ENABLED: "false"`) — metadata/lineage ingestion is CLI-only, run inside the `jupyter` container (which already has network access to both `trino` and `openmetadata-server`, plus the mounted `dbt/` project and `config/`). Two-step, order matters: `config/openmetadata/ingest-trino.yaml` first registers the `raw`/`staging`/`curated`/`trusted` schemas as Database Service `trino_lakehouse`, then `config/openmetadata/ingest-dbt.yaml` attaches lineage/column-docs/test-results from `dbt/target/{manifest,catalog,run_results}.json` onto those already-registered tables (dbt ingestion does not create table entities itself). Both YAMLs interpolate `${OPENMETADATA_JWT_TOKEN}` via `envsubst` at run time rather than storing the bot token in the repo — same pattern as `config/jupyter/before-spark-conf.sh`. Full walkthrough: `docs/OPENMETADATA-SETUP.md`.

## Architecture

### Storage/compute separation, and two distinct catalog protocols
Spark and Trino share no compute — they only communicate through MinIO (storage) and Nessie (catalog). More subtly, **there are two different ways to talk to Nessie as an Iceberg catalog**, and code in this repo uses both:

1. **Nessie's native catalog protocol** — used by Spark (`notebooks/spark_init.py`, `spark.sql.catalog.nessie.type=nessie`) and Trino (`config/trino/catalog/nessie.properties`, `iceberg.catalog.type=nessie`). Each engine brings its own S3 client config (Hadoop S3A for Spark, `s3.*` properties for Trino) and talks to Nessie's `/api/v1` or `/api/v2` endpoint.
2. **The generic Apache Iceberg REST Catalog spec** — used by DuckDB's `iceberg` extension and `pyiceberg`'s `rest` catalog type (both used in the DE1 subproject). This hits Nessie's `/iceberg` endpoint, which required explicit server-side object-store configuration to work at all (`config/nessie/application.properties` — `nessie.catalog.default-warehouse`, `nessie.catalog.service.s3.default-options.*`). Without it, any REST-catalog client fails immediately with "Warehouse not known".

Tables created via path 1 with `s3a://` locations are **not** readable via path 2 (Nessie's built-in Iceberg-REST `S3FileIO` only understands `s3://`, not Hadoop's `s3a://`) — this only matters if something needs to cross-read a Spark-written table through DuckDB/pyiceberg; Trino reads both schemes fine.

### Medallion layers and the raw-layer provenance pattern
`raw` (`s3://raw/`, Spark, append-only) → `staging` (`s3://staging/`, dbt/Trino) → `curated` (`s3://curated/`, dbt) → `trusted` (dbt, gated — see below). Raw tables are never overwritten, only appended; history lives in Iceberg snapshots, not in the raw schema.

Raw ingestion for semi-structured sources (NZDPU JSON, CDP CSV, Fondspreise JSON) follows a consistent **file-level** pattern, not record-level: one Iceberg row per ingested *file*, columns `ingestion_id`, `ingestion_timestamp`, `source_system`, `source_file_path`, `source_file_hash` (sha256, must match a re-hash of `raw_payload`), `source_file_format`, and `raw_payload` (the full file content as a string, byte-identical to the source). Parsing/expansion into rows happens later in **staging**, via SQL string-splitting in Trino (see `dbt/models/staging/stg_cdp_emissions.sql`: `split(raw_payload, chr(10))` + `split_part`), not at ingest time. When adding a new raw source, follow this same file-level shape (see `scripts/init-nzdpu-table.py` / `scripts/ingest-nzdpu.py` as the reference pair) rather than parsing at ingestion.

Raw-layer tables get an **explicit** location under their bucket (`s3a://raw/<table>`) rather than living in the default warehouse (`s3://warehouse/`) — set via `.tableProperty("location", ...)` at create time. Preserve this when adding tables.

### The quality gate is not `dbt test`
`trusted_esg_emissions` is a 1:1 passthrough of `curated_esg_emissions` with *stricter* dbt tests (e.g. `not_null` on `scope_1_tco2e`, which curated allows to be null). It is intentionally written **without** a `WHERE` filter, so that `dbt test` on it goes red if the promotion gate was bypassed — that's the didactic point, not a bug. The real gate is `scripts/promote-trusted-esg.py`: dbt-refresh curated → run a Great Expectations checkpoint (`great_expectations/checkpoints/curated_esg_checkpoint.yml`, suite `curated_esg_emissions_suite`, via the `trino_lakehouse` GE datasource) → only on green, dbt-run trusted. dbt itself runs inside the `jupyter` container; Great Expectations runs locally in the host's `uv` environment — the script shells out to `docker compose exec` for the dbt phases.

### dbt schema naming is overridden
`dbt/macros/generate_schema_name.sql` replaces dbt's default behavior: custom schemas (`staging`, `curated`, `trusted`) are used **literally**, not prefixed with `<target_schema>_` (dbt's normal `<target>_<custom>` convention is disabled). Keep this in mind when adding models with a `+schema` config.

### Sandbox → production mapping
| Component | Sandbox | Production (example) |
|---|---|---|
| Object storage | MinIO | FI-TS S3 / AWS S3 |
| Iceberg catalog | Nessie (REST) | Polaris / Nessie Enterprise |
| SQL engine | Trino | Trino / Athena / Snowflake |
| Processing | Spark (local) | Spark on Kubernetes / EMR |
| Notebooks | Jupyter (Docker) | JupyterHub / SageMaker |
| Transforms | dbt Core | dbt Cloud / dbt Core |

Nessie is used over Polaris in the sandbox specifically for its browsable web UI (table/branch/commit state) — both implement the same Iceberg REST Catalog protocol, so switching is not a Spark/Trino code change.

## Known gaps / gotchas

- **CRLF line endings** break shell scripts under Windows/WSL2 (`.gitattributes` forces LF, but check this first on any "container won't boot" report).
- **DuckDB against Nessie's Iceberg-REST endpoint requires `ACCESS_DELEGATION_MODE 'none'`** in `ATTACH` — the default `vended_credentials` mode causes Nessie to hand out S3 credentials that MinIO rejects with HTTP 403 on write (metadata commits succeed, data-file writes don't). `pyiceberg` doesn't have this problem since it uses its own passed-in `s3.access-key-id`/`s3.secret-access-key` for writes by default.
- **`nessie.catalog.service.s3.default-options.access-key` (the `urn:nessie-secret:quarkus:...` indirection) does not reliably resolve when set via Docker Compose environment variables** on Nessie 0.99.0 (upstream issue `projectnessie/nessie#11759`) — it works when supplied via a mounted `application.properties` (`QUARKUS_CONFIG_LOCATIONS`), which is why `config/nessie/application.properties` is a mounted file rather than more `environment:` entries in `docker-compose.yml`.
- `make pull` only pulls services with a plain `image:` key; `spark-master`/`spark-worker`/`jupyter` are `build:`-based, so their base images and the Iceberg/Hadoop/AWS JARs (downloaded from Maven Central during build) are only fetched on `docker compose build`/`up`. A full offline-readiness pass needs an actual `docker compose up -d`, not just `make pull`.
- Catalog/S3 connection properties are hardcoded per-file (`config/trino/catalog/nessie.properties`, `config/nessie/application.properties`) rather than templated from `.env`, unlike `docker-compose.yml` itself — if you change credentials or ports in `.env`, these files need manual updates too.
- **OpenMetadata basic-auth login is by email, not username** — `docker-compose.openmetadata.yml` sets no `AUTHORIZER_PRINCIPAL_DOMAIN`, so the seeded admin account is `admin@open-metadata.org` / `admin` (verified against the bcrypt hash in `user_entity`), not plain `admin`. Logging in with just `admin` gets misread as an invalid email and can push the UI into self-signup instead, producing a new non-admin user.
- **`dbt/target/catalog.json` is not produced by `dbt run`/`dbt build`**, only by `dbt docs generate` — the OpenMetadata dbt-ingestion workflow needs it for column-level lineage; without it, lineage still works but column docs don't.
- `DAGSTER_DB` is defined in `.env` but there is no `dagster` service in `docker-compose.yml` — vestigial/planned config, not wired up to anything yet.
- **`make health` calls `scripts/healthcheck.sh`, which does not exist in the repo** (verified — `Makefile` references it, the file is absent). The target currently fails; don't rely on it or point users to it until the script is added.
