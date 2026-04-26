"""
ingest-nzdpu.py — Raw Layer Ingestion (Variante B): eine JSON-Datei = ein Iceberg-Row.

raw_payload enthaelt den KOMPLETTEN JSON-Inhalt als UTF-8-String — byte-identisch
zum Original (inkl. Whitespace, Key-Reihenfolge, evtl. mojibake-artigen Sequenzen
in _meta.note). sha256(raw_payload.encode("utf-8")) muss immer == source_file_hash sein.

JSON wird ausschliesslich geparst, um source_system aus dem Wrapper-Feld "source"
zu extrahieren — die data[] / reporting_periods[]-Expansion ist Staging-Aufgabe.

Ausfuehrung im spark-master Container:
  docker compose exec spark-master \\
    spark-submit /scripts/ingest-nzdpu.py \\
      --file /data/sample/nzdpu_emissions.json \\
      --ingestion-timestamp 2026-04-20T08:30:00Z
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

TABLE = "nessie.raw.nzdpu_emissions"
SOURCE_FORMAT = "json"
DEFAULT_SOURCE_SYSTEM = "nzdpu"
DEFAULT_VERSION = "v1"


def eprint(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def build_spark() -> SparkSession:
    spark = SparkSession.builder.appName("nzdpu-ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


SCHEMA = StructType([
    StructField("ingestion_id",           StringType(),    nullable=False),
    StructField("ingestion_timestamp",    TimestampType(), nullable=False),
    StructField("source_system",          StringType(),    nullable=False),
    StructField("source_version",         StringType(),    nullable=True),
    StructField("source_file_path",       StringType(),    nullable=False),
    StructField("source_file_hash",       StringType(),    nullable=False),
    StructField("source_file_size_bytes", LongType(),      nullable=False),
    StructField("source_file_format",     StringType(),    nullable=False),
    StructField("raw_payload",            StringType(),    nullable=False),
])


def parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest NZDPU JSON (file-level) into nessie.raw.nzdpu_emissions"
    )
    parser.add_argument("--file", required=True, help="Path to JSON source file")
    parser.add_argument(
        "--ingestion-timestamp",
        default=None,
        metavar="ISO8601",
        help="UTC timestamp for this run, e.g. 2026-04-20T08:30:00Z (default: now)",
    )
    parser.add_argument(
        "--source-version",
        default=DEFAULT_VERSION,
        help=f"Source version label (default: {DEFAULT_VERSION!r}). "
             "NZDPU-JSON enthaelt keine version im Wrapper — manuell setzen.",
    )
    args = parser.parse_args()

    source_path = Path(args.file)
    if not source_path.exists():
        eprint(f"File not found: {source_path}")
        sys.exit(1)

    # Step 1: Bytes lesen — alles weitere leitet sich davon ab
    raw_bytes = source_path.read_bytes()
    file_size = len(raw_bytes)
    file_hash = hashlib.sha256(raw_bytes).hexdigest()

    log(f"Reading: {source_path}")
    log(f"File size: {file_size:,} bytes")
    log(f"File hash: sha256:{file_hash}")

    # Step 2: UTF-8-Dekodierung — raw_payload ist der dekodierte String, nicht re-serialisiert.
    #         Mojibake in _meta.note (siehe ESG-DATA-INVENTORY.md) bleibt erhalten.
    try:
        raw_payload = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        eprint(f"File is not valid UTF-8: {source_path} ({exc})")
        sys.exit(1)

    # Step 3: JSON parsen NUR fuer Wrapper-Metadaten — kein Re-Serialisieren.
    try:
        envelope = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        eprint(f"JSON parse failed: {exc}")
        sys.exit(1)

    if not isinstance(envelope, dict):
        eprint(
            "NZDPU-Wrapper erwartet (Objekt mit 'source' und 'data'), "
            f"aber Top-Level ist {type(envelope).__name__}."
        )
        sys.exit(1)

    source_system = envelope.get("source") or DEFAULT_SOURCE_SYSTEM

    log(f"Source system: {source_system}")
    log(f"Source version: {args.source_version}")
    log(f"Source format: {SOURCE_FORMAT}")
    log("")

    # Step 4: Ingestion-Metadaten
    ingestion_id = str(uuid.uuid4())
    ingestion_ts = (
        parse_ts(args.ingestion_timestamp)
        if args.ingestion_timestamp
        else datetime.now(tz=timezone.utc)
    )

    log(f"Ingestion ID: {ingestion_id}")
    log(f"Ingestion timestamp: {ingestion_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    log("")

    # Step 5: ein einziger Row schreiben
    row = (
        ingestion_id,
        ingestion_ts,
        source_system,
        args.source_version,
        str(source_path.resolve()),
        f"sha256:{file_hash}",
        file_size,
        SOURCE_FORMAT,
        raw_payload,
    )

    spark = build_spark()
    df = spark.createDataFrame([row], schema=SCHEMA)

    log(f"Writing to Iceberg: {TABLE}")
    df.writeTo(TABLE).append()
    log("Records written: 1 (file-level)")
    log("")

    snap_count = (
        spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.snapshots")
        .collect()[0]["cnt"]
    )
    log(f"Current snapshot count for raw.nzdpu_emissions: {snap_count}")

    spark.stop()


if __name__ == "__main__":
    main()
