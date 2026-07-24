"""
init-mitarbeiterzahlen-table.py — Lege nessie.raw.mitarbeiterzahlen im File-level
Schema an.

Kleinste moegliche neue Raw-Quelle, um den Weg Raw -> Staging -> Curated end-to-end
durchzuspielen: eine CSV-Datei = ein Iceberg-Row mit raw_payload als String
(gleiches Muster wie cdp_emissions, siehe init-cdp-table.py).

Idempotent: zweimalige Ausfuehrung wirft keinen Fehler.

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/init-mitarbeiterzahlen-table.py
"""

import sys

from pyspark.sql import SparkSession

TABLE = "nessie.raw.mitarbeiterzahlen"
RAW_LOCATION = "s3a://raw/mitarbeiterzahlen"


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("init-mitarbeiterzahlen-table").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

    existing = spark.sql("SHOW TABLES IN nessie.raw LIKE 'mitarbeiterzahlen'").collect()
    if existing:
        log(f"Tabelle {TABLE} existiert bereits — nichts zu tun.")
        spark.stop()
        return

    spark.sql(f"""
        CREATE TABLE {TABLE} (
            ingestion_id           STRING    NOT NULL,
            ingestion_timestamp    TIMESTAMP NOT NULL,
            source_system          STRING    NOT NULL,
            source_version         STRING,
            source_file_path       STRING    NOT NULL,
            source_file_hash       STRING    NOT NULL,
            source_file_size_bytes BIGINT    NOT NULL,
            source_file_format     STRING    NOT NULL,
            raw_payload            STRING    NOT NULL
        )
        USING ICEBERG
        PARTITIONED BY (days(ingestion_timestamp))
        LOCATION '{RAW_LOCATION}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
    """)
    log(f"Tabelle {TABLE} angelegt (File-level Schema, Iceberg v2).")

    cols = spark.sql(f"DESCRIBE {TABLE}").collect()
    log(f"Spalten: {len(cols)}")

    spark.stop()


if __name__ == "__main__":
    main()
