"""
init-nzdpu-table.py — Droppe + lege nessie.raw.nzdpu_emissions im File-level Schema neu an.

Die alte Tabelle (Record-level Ingestion via spark-ingestion.py /
spark-ingestion-v2.py) wird verworfen. Das neue Schema entspricht
Variante B (eine JSON-Datei = ein Iceberg-Row mit raw_payload als String)
und ist strukturell identisch zu raw.cdp_emissions.

Idempotent: zweimalige Ausfuehrung wirft keinen Fehler.

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/init-nzdpu-table.py
"""

import sys

from pyspark.sql import SparkSession

TABLE = "nessie.raw.nzdpu_emissions"
RAW_LOCATION = "s3a://raw/nzdpu_emissions"


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("init-nzdpu-table").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")

    existing = spark.sql("SHOW TABLES IN nessie.raw LIKE 'nzdpu_emissions'").collect()
    if existing:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        log(f"Tabelle {TABLE} gedropped (alte Record-level Daten verworfen).")
    else:
        log(f"Tabelle {TABLE} existierte nicht — neu anlegen.")

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
