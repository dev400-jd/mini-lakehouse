"""
        !!! UNTESTED !!!
duckdb-ingestion.py — Raw Layer Ingestion (DuckDB-Variante): yellow_tripdata_2009-01.parquet -> Iceberg via Nessie REST Catalog

Pendant zu spark-ingestion.py, aber fuer DuckDB 1.5.5 statt Spark.

WICHTIG (siehe Erklaerung im Chat):
  - DuckDB's iceberg-Extension ist laut DuckDB-Doku "currently in an experimental
    state" -- als Einzelnutzer-Ad-hoc-Pfad gedacht, nicht als Ersatz fuer
    Spark's Iceberg-Writer in produktiven ETL-Jobs.
  - Der Warehouse-Name im ATTACH-Statement ('raw') ist eine ANNAHME basierend
    auf der Bucket-Namenskonvention in .env (S3_BUCKET_RAW=raw). Bitte gegen
    die tatsaechliche Nessie-Server-Config pruefen (nessie.catalog.warehouses.*).
  - TOKEN '' ist ein bekannter Workaround fuer Nessie-Instanzen ohne aktivierte
    Authentifizierung (nessie.server.authentication.enabled=false).

Ausfuehrung (innerhalb eines Containers im lakehouse-Docker-Netzwerk, z.B.
Jupyter-Kernel oder ein dediziertes duckdb-Utility-Image):
  python /scripts/duckdb-ingestion.py

Falls das Skript stattdessen vom Host laeuft (nicht aus einem Compose-Service
heraus), muessen 'minio' und 'nessie' unten durch 'host.docker.internal'
ersetzt werden.
"""

import os
import time
import duckdb

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

DATA_DIR = "/data/yellow_taxi"
SOURCE_FILE = f"{DATA_DIR}/yellow_tripdata_2009-01.parquet"

MINIO_ENDPOINT_HOST = "minio:9000"          # host.docker.internal:9000 falls vom Docker-Host aus
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse123")

NESSIE_REST_ENDPOINT = "http://nessie:19120/iceberg"  # host.docker.internal:19120 falls vom Docker-Host aus
NESSIE_WAREHOUSE = "raw"    # ANNAHME -- gegen Nessie-Server-Config pruefen!

CATALOG_ALIAS = "nessie_raw"
TARGET_SCHEMA = f"{CATALOG_ALIAS}.raw"
TARGET_TABLE = f"{TARGET_SCHEMA}.yellow_taxi_200901_duck"


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


# ---------------------------------------------------------------------------
# SETUP: Extensions, Secrets, Catalog-Attach
# ---------------------------------------------------------------------------

print("\n[SETUP] DuckDB-Verbindung und Extensions ...", flush=True)
con = duckdb.connect(database=":memory:")

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")
log(f"DuckDB Version: {duckdb.__version__}")

# S3-Secret fuer MinIO (Object-Storage-Zugriff der Iceberg-Datendateien)
con.execute(f"""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        ENDPOINT '{MINIO_ENDPOINT_HOST}',
        URL_STYLE 'path',
        USE_SSL false
    );
""")
log("S3-Secret fuer MinIO angelegt (path-style, kein SSL)")

# Iceberg-Secret fuer Nessie REST Catalog (kein OAuth2, Auth ist deaktiviert)
con.execute("""
    CREATE OR REPLACE SECRET nessie_iceberg_secret (
        TYPE ICEBERG,
        TOKEN ''
    );
""")
log("Iceberg-Secret angelegt (TOKEN '' -- Nessie ohne Authentifizierung)")

# Catalog attachen
con.execute(f"""
    ATTACH '{NESSIE_WAREHOUSE}' AS {CATALOG_ALIAS} (
        TYPE ICEBERG,
        ENDPOINT '{NESSIE_REST_ENDPOINT}',
        SECRET nessie_iceberg_secret
    );
""")
log(f"Nessie REST Catalog attached als '{CATALOG_ALIAS}' (warehouse='{NESSIE_WAREHOUSE}', endpoint='{NESSIE_REST_ENDPOINT}')")

print("\n[SETUP] Namespace und Cleanup ...", flush=True)
con.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
log(f"Schema {TARGET_SCHEMA} bereit")

con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE};")
log(f"DROP TABLE IF EXISTS {TARGET_TABLE}")


# ---------------------------------------------------------------------------
# INGESTION yellow_tripdata_2009-01.parquet -> nessie.raw.yellow_taxi_200901_duck
# ---------------------------------------------------------------------------

print(f"\n{SOURCE_FILE} -> {TARGET_TABLE}", flush=True)
log("CTAS aus read_parquet(): DuckDB leitet Schema automatisch ab")
log("Hinweis: kein explizites Setzen einer Custom-Location wie bei Spark --")
log("         die physische Location wird ueber die Nessie-Warehouse-Config gesteuert")

log("Schema-Vorschau (erste 8 Spalten):")
preview = con.execute(f"""
    SELECT * FROM read_parquet('{SOURCE_FILE}') LIMIT 0
""").description
for col in preview[:8]:
    print(f"    {col[0]}: {col[1]}", flush=True)

start = time.perf_counter()

con.execute(f"""
    CREATE TABLE {TARGET_TABLE} AS
    SELECT * FROM read_parquet('{SOURCE_FILE}')
""")

elapsed = time.perf_counter() - start

cnt = con.execute(f"SELECT count(*) AS cnt FROM {TARGET_TABLE}").fetchone()[0]

print(f"  -> {TARGET_TABLE}: {cnt} Zeilen geladen", flush=True)
print(f"  -> Ingestion-Dauer: {elapsed:.2f}s ({elapsed/60:.2f}min)", flush=True)


# ---------------------------------------------------------------------------
# Abschlusskontrolle
# ---------------------------------------------------------------------------

print("\n" + "=" * 60, flush=True)
print("  DuckDB Raw Layer Ingestion abgeschlossen", flush=True)
print("=" * 60, flush=True)

print(f"\nAlle Tabellen in {TARGET_SCHEMA}:", flush=True)
con.sql(f"SHOW TABLES FROM {TARGET_SCHEMA};").show()

print("\nZusammenfassung:", flush=True)
print(f"  {'Tabelle':<45} {'Zeilen':>10} {'Dauer (s)':>10}", flush=True)
print(f"  {'-'*67}", flush=True)
print(f"  {'raw.yellow_taxi_200901_duck':<45} {cnt:>10} {elapsed:>10.2f}", flush=True)

con.close()