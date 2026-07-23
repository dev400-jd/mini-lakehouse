"""
spark-ingestion.py — Raw Layer Ingestion: geparste Referenztabellen -> Iceberg via Nessie

Laedt die Quelldateien, die im Raw Layer geparst (Record-level) vorliegen sollen:
  - owid_co2_countries  (saubere Referenzdaten, partitioniert nach year)
  - fund_master         (kleine Lookup-Tabelle)
  - fund_positions      (partitioniert nach position_date)

cdp_emissions und nzdpu_emissions werden NICHT hier geladen. Sie liegen als
File-level Raw (eine Quelldatei = ein Iceberg-Row mit raw_payload) vor und
werden ueber init-cdp-table.py / ingest-cdp.py bzw. init-nzdpu-table.py /
ingest-nzdpu.py erzeugt (in seed-data.sh direkt nach diesem Skript). Die
dbt-Staging-Modelle stg_cdp_emissions / stg_nzdpu_emissions erwarten die
File-level-Form.

Ausfuehrung:
  docker compose exec spark-master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /scripts/spark-ingestion.py

Prinzip Raw Layer:
  - Daten wie sie sind: minimale technische Anpassungen, keine fachliche Bereinigung
  - Typecasts und Bereinigung kommen im Staging Layer (dbt)
  - Partitionierung nur wo es Sinn ergibt (grosse Tabellen mit klarem Partitionsschluessel)
  - Jede Tabelle hat eine explizite Location im raw-Bucket (s3a://raw/<tablename>)
    statt im Default-Warehouse (s3a://warehouse/)
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("raw-layer-ingestion")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/data/yellow_taxi"
RAW_BUCKET = "s3a://raw"
RESULTS = []   # (tabelle, zeilenanzahl)


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def verify_and_record(table: str) -> int:
    cnt = spark.sql(f"SELECT count(*) AS cnt FROM {table}").collect()[0]["cnt"]
    print(f"  -> {table}: {cnt} Zeilen geladen", flush=True)
    RESULTS.append((table, cnt))
    return cnt


# ---------------------------------------------------------------------------
# SETUP: Namespace sicherstellen, Altlasten droppen
# ---------------------------------------------------------------------------

print("\n[SETUP] Namespace und Cleanup ...", flush=True)
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
log("Namespace nessie.raw bereit")

TABLES_TO_DROP = [
    "nessie.raw.yellowtripdata",
]
for tbl in TABLES_TO_DROP:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    log(f"DROP TABLE IF EXISTS {tbl}")

log("Cleanup abgeschlossen")


# ---------------------------------------------------------------------------
# INGESTION yellow_tripdata_2010-05.parquet -> nessie.raw.yellowtripdata
# ---------------------------------------------------------------------------

print("\nyellow_tripdata_2010-05.parquet -> nessie.raw.yellowtripdata", flush=True)
log("inferSchema=True: Daten sind sauber genug fuer automatische Typerkennung")
log("Iceberg Hidden Partitioning nach 'year'")
log(f"Location: {RAW_BUCKET}/yellowtripdata")

taxi_df = (
    spark.read.parquet(f"{DATA_DIR}/yellow_tripdata_2010-05.parquet")
)

log("Schema (erste 8 Spalten):")
taxi_df.select(taxi_df.columns[:8]).printSchema()

(
    taxi_df.writeTo("nessie.raw.yellowtripdata")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/yellowtripdata")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.yellowtripdata").show()
verify_and_record("nessie.raw.yellowtripdata")

# ---------------------------------------------------------------------------
# Abschlusskontrolle
# ---------------------------------------------------------------------------

print("\n" + "=" * 60, flush=True)
print("  Raw Layer Ingestion abgeschlossen", flush=True)
print("=" * 60, flush=True)

print("\nAlle Tabellen in nessie.raw:", flush=True)
spark.sql("SHOW TABLES IN nessie.raw").show()

print("\nZusammenfassung:", flush=True)
print(f"  {'Tabelle':<40} {'Zeilen':>8}", flush=True)
print(f"  {'-'*48}", flush=True)
for table, cnt in RESULTS:
    short = table.replace("nessie.raw.", "raw.")
    print(f"  {short:<40} {cnt:>8}", flush=True)

total = sum(cnt for _, cnt in RESULTS)
print(f"  {'-'*48}", flush=True)
print(f"  {'GESAMT':<40} {total:>8}", flush=True)

spark.stop()
