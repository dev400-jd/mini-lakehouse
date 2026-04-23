"""
ingest-fondspreise.py — Raw Layer Ingestion (Variante B): eine Datei = ein Iceberg-Row.

raw_payload enthaelt den KOMPLETTEN Datei-Inhalt als UTF-8-String — byte-identisch
zum Original. sha256(raw_payload.encode("utf-8")) muss immer == source_file_hash sein.

Das JSON-Parsing dient ausschliesslich der Extraktion der Provenance-Felder
source_system und source_version aus dem Wrapper. Einzelne Records werden hier
NICHT expandiert — das ist Staging-Aufgabe.

Ausfuehrung im spark-master Container:
  docker compose exec spark-master \\
    spark-submit /scripts/ingest-fondspreise.py \\
      --file /data/sample/fondspreise_load1.json \\
      --ingestion-timestamp 2026-04-20T08:15:00Z
"""

import argparse
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TABLE = "nessie.raw.fondspreise"
RAW_LOCATION = "s3a://raw/fondspreise"


def eprint(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def build_spark() -> SparkSession:
    spark = SparkSession.builder.appName("fondspreise-ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ingestion_id           STRING    NOT NULL,
            ingestion_timestamp    TIMESTAMP NOT NULL,
            source_system          STRING    NOT NULL,
            source_version         STRING,
            source_file_path       STRING    NOT NULL,
            source_file_hash       STRING    NOT NULL,
            source_file_size_bytes BIGINT    NOT NULL,
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


SCHEMA = StructType([
    StructField("ingestion_id",           StringType(),    nullable=False),
    StructField("ingestion_timestamp",    TimestampType(), nullable=False),
    StructField("source_system",          StringType(),    nullable=False),
    StructField("source_version",         StringType(),    nullable=True),
    StructField("source_file_path",       StringType(),    nullable=False),
    StructField("source_file_hash",       StringType(),    nullable=False),
    StructField("source_file_size_bytes", LongType(),      nullable=False),
    StructField("raw_payload",            StringType(),    nullable=False),
])


def parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest fondspreise JSON (file-level) into nessie.raw.fondspreise"
    )
    parser.add_argument("--file", required=True, help="Path to JSON source file")
    parser.add_argument(
        "--ingestion-timestamp",
        default=None,
        metavar="ISO8601",
        help="UTC timestamp for this run, e.g. 2026-04-20T08:15:00Z (default: now)",
    )
    args = parser.parse_args()

    source_path = Path(args.file)
    if not source_path.exists():
        eprint(f"File not found: {source_path}")
        sys.exit(1)

    # Step 1: read bytes — alles weitere leitet sich davon ab
    raw_bytes = source_path.read_bytes()

    file_size = len(raw_bytes)
    file_hash = hashlib.sha256(raw_bytes).hexdigest()

    log(f"Reading: {source_path}")
    log(f"File size: {file_size:,} bytes")
    log(f"File hash: sha256:{file_hash}")

    # Step 2: UTF-8-Dekodierung — raw_payload ist der dekodierte String, nicht re-serialisiert
    try:
        raw_payload = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        eprint(f"File is not valid UTF-8: {exc}")
        sys.exit(1)

    # Step 3: JSON parsen NUR fuer Wrapper-Metadaten (source_system, source_version)
    try:
        envelope = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        eprint(f"JSON parse failed: {exc}")
        sys.exit(1)

    source_system  = envelope.get("source_system", "")
    source_version = envelope.get("source_version")

    log(f"Parsed source_system:  {source_system}")
    log(f"Parsed source_version: {source_version}")
    log("")

    # Step 4: Ingestion-Metadaten
    ingestion_id = str(uuid.uuid4())
    ingestion_ts = (
        parse_ts(args.ingestion_timestamp)
        if args.ingestion_timestamp
        else datetime.now(tz=timezone.utc)
    )

    log(f"Ingestion ID:        {ingestion_id}")
    log(f"Ingestion timestamp: {ingestion_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    log("")

    # Step 5: einen einzigen Row schreiben
    row = (
        ingestion_id,
        ingestion_ts,
        source_system,
        source_version,
        str(source_path.resolve()),
        f"sha256:{file_hash}",
        file_size,
        raw_payload,
    )

    spark = build_spark()
    ensure_table(spark)

    df = spark.createDataFrame([row], schema=SCHEMA)

    log(f"Writing to Iceberg: {TABLE}")
    df.writeTo(TABLE).append()
    log("Records written: 1 (file-level)")
    log("")

    snap_count = (
        spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.snapshots")
        .collect()[0]["cnt"]
    )
    log(f"Current snapshot count for raw.fondspreise: {snap_count}")

    spark.stop()


if __name__ == "__main__":
    main()
