"""
spark-ingestion.py — Raw Layer Ingestion: Beispieldaten -> Iceberg via Nessie

Laedt 5 Quelldateien aus /data/sample/ und schreibt sie als Iceberg-Tabellen
in den Raw Layer (nessie.raw.*). Demonstriert Spark's Staerken bei verschiedenen
Quellformaten: nested JSON, dreckige CSV, saubere CSV, kleine Lookup-Tabellen.

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

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

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

# Alle bestehenden Raw-Tabellen droppen damit Location-Property neu gesetzt werden kann.
# createOrReplace() aendert keine Location bei existierenden Tabellen.
TABLES_TO_DROP = [
    "nessie.raw.nzdpu_emissions",
    "nessie.raw.cdp_emissions",
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
# INGESTION 1 — NZDPU JSON (nested)
# ---------------------------------------------------------------------------

print("\n[1/5] NZDPU Emissions JSON -> nessie.raw.nzdpu_emissions", flush=True)
log("spark.read.json mit multiline=true (API-Response-Struktur)")
log(f"Location: {RAW_BUCKET}/nzdpu_emissions")

raw_nzdpu = (
    spark.read
    .option("multiline", "true")
    .json(f"{DATA_DIR}/nzdpu_emissions.json")
)

# Top-Level: {status, total_records, source, data: [...]}
# data[] enthaelt Unternehmens-Objekte, jedes mit reporting_periods[]
# Ergebnis: 1 Zeile pro Unternehmen x Berichtsjahr (30 Unternehmen x 3 Jahre = 90 Zeilen)

companies = raw_nzdpu.select(explode("data").alias("co"))
nzdpu_df = (
    companies
    .select(
        col("co.reporting_periods"),
        col("co.company_id"),
        col("co.company_name"),
        col("co.isin"),
        col("co.lei"),
        col("co.country_of_incorporation").alias("country"),
        col("co.primary_sector").alias("sector"),
    )
    .select(
        explode("reporting_periods").alias("period"),
        col("company_id"),
        col("company_name"),
        col("isin"),
        col("lei"),
        col("country"),
        col("sector"),
    )
    .select(
        col("company_id"),
        col("company_name"),
        col("isin"),
        col("lei"),
        col("country"),
        col("sector"),
        col("period.reporting_year").alias("reporting_year"),
        col("period.scope_1.value").alias("scope_1_tco2e"),
        col("period.scope_2_location_based.value").alias("scope_2_location_tco2e"),
        col("period.scope_2_market_based.value").alias("scope_2_market_tco2e"),
        col("period.verification_status"),
        col("period.reporting_framework"),
    )
)

log("Schema:")
nzdpu_df.printSchema()

(
    nzdpu_df.writeTo("nessie.raw.nzdpu_emissions")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/nzdpu_emissions")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.nzdpu_emissions").show()
verify_and_record("nessie.raw.nzdpu_emissions")


# ---------------------------------------------------------------------------
# INGESTION 2 — CDP CSV (dreckige Realdaten, alles als String)
# ---------------------------------------------------------------------------

print("\n[2/5] CDP Emissions CSV -> nessie.raw.cdp_emissions", flush=True)
log("inferSchema=False: alles als String — Typcasts kommen im Staging Layer")
log(f"Location: {RAW_BUCKET}/cdp_emissions")

cdp_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")   # fehlerhafte Zeilen als null, nicht verwerfen
    .csv(f"{DATA_DIR}/cdp_emissions.csv")
)

log(f"Spalten ({len(cdp_df.columns)}): {', '.join(cdp_df.columns)}")

(
    cdp_df.writeTo("nessie.raw.cdp_emissions")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("location", f"{RAW_BUCKET}/cdp_emissions")
    .create()
)

spark.sql("SELECT count(*) AS cnt FROM nessie.raw.cdp_emissions").show()
verify_and_record("nessie.raw.cdp_emissions")


# ---------------------------------------------------------------------------
# INGESTION 3 — OWID CO2 Countries (saubere Referenzdaten, mit Partitionierung)
# ---------------------------------------------------------------------------

print("\n[3/5] OWID CO2 Countries CSV -> nessie.raw.owid_co2_countries", flush=True)
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
# INGESTION 4 — Fund Master (kleine Lookup-Tabelle, keine Partitionierung)
# ---------------------------------------------------------------------------

print("\n[4/5] Fund Master CSV -> nessie.raw.fund_master", flush=True)
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
# INGESTION 5 — Fund Positions (partitioniert nach position_date)
# ---------------------------------------------------------------------------

print("\n[5/5] Fund Positions CSV -> nessie.raw.fund_positions", flush=True)
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
