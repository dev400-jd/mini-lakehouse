"""
spark-ingestion-v2.py — Appended NZDPU API v2 Daten in den Raw Layer

Liest data/sample/nzdpu_emissions_v2.json (verschachtelte V2-Struktur),
flattened die Felder und appended an nessie.raw.nzdpu_emissions.

Fügt fehlende Spalten (net_zero_target_year, scope_3_total_tco2e) per
ALTER TABLE hinzu, falls sie noch nicht existieren.

Ausführung:
  docker compose exec spark-master \\
    /opt/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      /scripts/spark-ingestion-v2.py

V2-Format (verschachtelt):
  {
    "entity": {"id", "name", "isin", "lei", "country"},
    "industry_classification": "...",
    "reporting_year": 2021,
    "emissions": {"scope_1_tco2e", "scope_2_location_tco2e", ...},
    "climate_target": {"net_zero_year": 2040},
    "metadata": {"reporting_framework", "verification", "api_version"}
  }
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("nzdpu-v2-ingestion")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/data/sample"
TABLE = "nessie.raw.nzdpu_emissions"


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


# ---------------------------------------------------------------------------
# Schritt 1: Zeilenanzahl vorher
# ---------------------------------------------------------------------------

count_before = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"\nZeilenanzahl vor Append: {count_before}", flush=True)


# ---------------------------------------------------------------------------
# Schritt 2: V2 JSON laden
# ---------------------------------------------------------------------------

print("\n[1/3] V2 JSON laden ...", flush=True)
df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(f"{DATA_DIR}/nzdpu_emissions_v2.json")
)
log(f"Eingelesene Einträge: {df_raw.count()}")
log("Schema (verschachtelt):")
df_raw.printSchema()


# ---------------------------------------------------------------------------
# Schritt 3: Flatten — nested → tabular
# ---------------------------------------------------------------------------

print("\n[2/3] Flattening ...", flush=True)
df_flat = df_raw.select(
    col("entity.id").alias("company_id"),
    col("entity.name").alias("company_name"),
    col("entity.isin").alias("isin"),
    col("entity.lei").alias("lei"),
    col("entity.country").alias("country"),
    col("industry_classification").alias("sector"),
    col("reporting_year"),
    col("emissions.scope_1_tco2e").alias("scope_1_tco2e"),
    col("emissions.scope_2_location_tco2e").alias("scope_2_location_tco2e"),
    col("emissions.scope_2_market_tco2e").alias("scope_2_market_tco2e"),
    col("metadata.verification").alias("verification_status"),
    col("metadata.reporting_framework").alias("reporting_framework"),
    col("climate_target.net_zero_year").cast("int").alias("net_zero_target_year"),
    col("emissions.scope_3_total_tco2e").alias("scope_3_total_tco2e"),
)
log(f"Flattened Zeilen: {df_flat.count()}")


# ---------------------------------------------------------------------------
# Schritt 4: Neue Spalten hinzufügen (idempotent)
# ---------------------------------------------------------------------------

print("\n[3/3] Schema Evolution + Append ...", flush=True)

existing = [r[0] for r in spark.sql(f"DESCRIBE {TABLE}").collect()]

if "net_zero_target_year" not in existing:
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (net_zero_target_year INT)")
    log("net_zero_target_year hinzugefügt")
else:
    log("net_zero_target_year existiert bereits")

if "scope_3_total_tco2e" not in existing and "scope_3_tco2e" not in existing:
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (scope_3_total_tco2e BIGINT)")
    log("scope_3_total_tco2e hinzugefügt")
else:
    log("scope_3 Spalte existiert bereits")


# ---------------------------------------------------------------------------
# Schritt 5: Append — Raw Layer ist append-only
# ---------------------------------------------------------------------------

table_cols = spark.sql(f"SELECT * FROM {TABLE} LIMIT 0").columns
df_flat.select(table_cols).writeTo(TABLE).append()

count_after = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"\nZeilenanzahl nach Append: {count_after}", flush=True)
print(f"Neue Zeilen: {count_after - count_before}", flush=True)
print("\nFertig.", flush=True)
