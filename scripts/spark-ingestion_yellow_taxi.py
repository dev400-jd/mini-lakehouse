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

DATA_DIR = "/data/sample"
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

# Nur die hier verwalteten geparsten Tabellen droppen, damit die
# Location-Property neu gesetzt werden kann (createOrReplace() aendert
# keine Location bei existierenden Tabellen).
# cdp_emissions / nzdpu_emissions werden NICHT angefasst — deren File-level
# Tabellen legen init-cdp-table.py / init-nzdpu-table.py selbst neu an.
TABLES_TO_DROP = [
    "nessie.raw.owid_co2_countries",
    "nessie.raw.fund_master",
    "nessie.raw.fund_positions",
    "nessie.raw.smoke_test",
]
for tbl in TABLES_TO_DROP:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    log(f"DROP TABLE IF EXISTS {tbl}")

log("Cleanup abgeschlossen")


# ---------------------------------------------------------------------------
# INGESTION 1 — OWID CO2 Countries (saubere Referenzdaten, mit Partitionierung)
# ---------------------------------------------------------------------------

print("\n[1/3] OWID CO2 Countries CSV -> nessie.raw.owid_co2_countries", flush=True)
log("inferSchema=True: OWID-Daten sind sauber genug fuer automatische Typerkennung")
log("Iceberg Hidden Partitioning nach 'year'")
log(f"Location: {RAW_BUCKET}/owid_co2_countries")

owid_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/owid_co2_countries.csv")
)

log("Schema (erste 8 Spalten):")
owid_df.select(owid_df.columns[:8]).printSchema()

(
    owid_df.writeTo("nessie.raw.owid_co2_countries")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/owid_co2_countries")
    .partitionedBy("year")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.owid_co2_countries").show()
verify_and_record("nessie.raw.owid_co2_countries")


# ---------------------------------------------------------------------------
# INGESTION 2 — Fund Master (kleine Lookup-Tabelle, keine Partitionierung)
# ---------------------------------------------------------------------------

print("\n[2/3] Fund Master CSV -> nessie.raw.fund_master", flush=True)
log("Kleine Lookup-Tabelle (10 Zeilen) — keine Partitionierung notwendig")
log(f"Location: {RAW_BUCKET}/fund_master")

fund_master_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/fund_master.csv")
)

log(f"Spalten: {', '.join(fund_master_df.columns)}")

(
    fund_master_df.writeTo("nessie.raw.fund_master")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/fund_master")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.fund_master").show()
verify_and_record("nessie.raw.fund_master")


# ---------------------------------------------------------------------------
# INGESTION 3 — Fund Positions (partitioniert nach position_date)
# ---------------------------------------------------------------------------

print("\n[3/3] Fund Positions CSV -> nessie.raw.fund_positions", flush=True)
log("Partitionierung nach 'position_date' (2 Stichtage = 2 Partitionen)")
log(f"Location: {RAW_BUCKET}/fund_positions")

fund_positions_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/fund_positions.csv")
)

log(f"Spalten: {', '.join(fund_positions_df.columns)}")

(
    fund_positions_df.writeTo("nessie.raw.fund_positions")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/fund_positions")
    .partitionedBy("position_date")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.fund_positions").show()
verify_and_record("nessie.raw.fund_positions")


# ---------------------------------------------------------------------------
# Abschlusskontrolle
# ---------------------------------------------------------------------------

print("\n" + "=" * 60, flush=True)
print("  Raw Layer Ingestion (geparste Referenztabellen) abgeschlossen", flush=True)
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
