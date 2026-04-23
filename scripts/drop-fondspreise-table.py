"""
drop-fondspreise-table.py — Droppe nessie.raw.fondspreise fuer Schema-Migration.

Idempotent: zweimalige Ausfuehrung wirft keinen Fehler.

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/drop-fondspreise-table.py
"""

import sys

from pyspark.sql import SparkSession


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("drop-fondspreise").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    table = "nessie.raw.fondspreise"

    existing = spark.sql("SHOW TABLES IN nessie.raw LIKE 'fondspreise'").collect()
    if existing:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        log(f"Tabelle {table} gedropped.")
    else:
        log(f"Tabelle {table} existierte nicht — nichts zu tun.")

    spark.stop()


if __name__ == "__main__":
    main()
