"""
taxi-landing-spark.py — Spark-Referenz-Landing fuer DE1 (Engine-Right-Sizing).

Baseline-Implementierung: laedt EINEN Monat NYC-TLC Yellow-Taxi-Parquet ueber
den nativen Spark-Iceberg-Connector nach nessie.raw.taxi_spark. Dient als
Referenz, gegen die die DuckDB- und Polars-Landing (Aufgabe der Gruppe,
raw.taxi_duckdb / raw.taxi_polars) verglichen werden — exakt dieselbe
Landing-Logik, nur die Engine variiert (siehe Briefing Abschnitt 3,
"die eine Variable").

Landing-Logik (bewusst minimal, aber mit echter Arbeit):
  Parquet lesen -> bekannte Schrott-Saetze filtern (Datumswerte ausserhalb
  des Zielmonats, negative Fares, Null-/fehlende Distanz) -> typisieren ->
  nach raw.taxi_spark schreiben.

Voraussetzung: Datei liegt unter /data/taxi/yellow_tripdata_<YYYY>-<MM>.parquet
(siehe: make fetch-taxi N=... bzw. scripts/fetch-taxi.sh).

Ausfuehrung:
  docker compose exec spark-master \\
    /opt/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      /scripts/taxi-landing-spark.py --year 2024 --month 1
"""

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

TABLE = "nessie.raw.taxi_spark"
RAW_LOCATION = "s3a://raw/taxi_spark"
DATA_DIR = "/data/taxi"


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def month_bounds(year: int, month: int) -> tuple[str, str]:
    """Erster Tag des Zielmonats (inklusive) und erster Tag des Folgemonats
    (exklusiv) — Grenze fuer den Datums-Filter."""
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start.isoformat(), end.isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark-Referenz-Landing: NYC-Taxi-Parquet -> nessie.raw.taxi_spark"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, help="1-12")
    args = parser.parse_args()

    ym = f"{args.year:04d}-{args.month:02d}"
    source_path = f"{DATA_DIR}/yellow_tripdata_{ym}.parquet"
    start_date, end_date = month_bounds(args.year, args.month)

    spark = SparkSession.builder.appName(f"taxi-landing-spark-{ym}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log(f"Quelle: {source_path}")
    log(f"Zielmonat-Fenster: [{start_date}, {end_date})")

    raw = spark.read.parquet(source_path)
    rows_in = raw.count()
    log(f"Gelesen: {rows_in:,} Zeilen")

    # --- Schrott-Saetze filtern ------------------------------------------------
    # NYC-TLC-Rohdaten enthalten regelmaessig: Erfassungsfehler mit Datumswerten
    # ausserhalb des angegebenen Monats, negative Fares (Storno-/Erstattungs-
    # buchungen) und Null-/fehlende Distanz. Fuer den Engine-Vergleich wird
    # bewusst simpel gefiltert — Datenqualitaets-Feintuning ist nicht Ziel von
    # DE1 (das ML-Problem soll banal bleiben, hier: der Vergleich selbst).
    filtered = (
        raw.where(to_date(col("tpep_pickup_datetime")).between(start_date, end_date))
        .where(col("fare_amount") >= 0)
        .where(col("trip_distance").isNotNull() & (col("trip_distance") > 0))
    )

    # --- Typisieren --------------------------------------------------------
    typed = (
        filtered.withColumn("VendorID", col("VendorID").cast("int"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("trip_distance", col("trip_distance").cast("double"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("PULocationID", col("PULocationID").cast("int"))
        .withColumn("DOLocationID", col("DOLocationID").cast("int"))
    )

    rows_out = typed.count()
    log(f"Nach Filter + Typisierung: {rows_out:,} Zeilen ({rows_in - rows_out:,} verworfen)")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

    (
        typed.writeTo(TABLE)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("location", RAW_LOCATION)
        .create()
    )

    log(f"Geschrieben: {TABLE} (Location: {RAW_LOCATION})")
    spark.sql(f"SELECT count(*) AS cnt FROM {TABLE}").show()

    spark.stop()


if __name__ == "__main__":
    main()
