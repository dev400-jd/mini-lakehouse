"""
verify-cdp-ingestion.py — Selbstpruefung von raw.cdp_emissions nach einem Ingest.

Erwartet nach erstem Load (Variante B — File-level Payload):
  - Rows gesamt:                   1 (oder mehr nach weiteren Loads)
  - sha256(raw_payload.encode()) == source_file_hash fuer jeden Row
  - len(raw_payload.encode("utf-8")) == source_file_size_bytes
  - source_file_format == "csv"
  - raw_payload ist als CSV parsebar (csv.reader auf StringIO)
  - Datenzeilen pro Row: 100 (101 Total inkl. Header)
  - Header-Zeile entspricht dem erwarteten 15-Spalten-Header

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/verify-cdp-ingestion.py
"""

import csv
import hashlib
import io
import sys

from pyspark.sql import SparkSession

TABLE = "nessie.raw.cdp_emissions"

EXPECTED_HEADER = (
    "Account Number,Organization,Primary Sector,Primary Industry,Country,"
    "ISIN,Reporting Year,Scope 1 (metric tons CO2e),"
    "Scope 2 Location-Based (metric tons CO2e),"
    "Scope 2 Market-Based (metric tons CO2e),"
    "Scope 3 Total (metric tons CO2e),Emission Unit,Data Verification,"
    "CDP Score,Public Disclosure"
)
EXPECTED_DATA_ROWS = 100


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def check(label: str, actual, expected, results: list) -> None:
    ok = actual == expected
    status = "OK  " if ok else "FAIL"
    log(f"[{status}] {label}: {actual} (erwartet: {expected})")
    results.append(ok)


def main() -> None:
    spark = SparkSession.builder.appName("verify-cdp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    print(f"\nVerifikation: {TABLE}", flush=True)
    print("=" * 56, flush=True)

    results = []

    total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
    if total < 1:
        log(f"[FAIL] Rows gesamt: {total} — keine Ingestion gefunden")
        spark.stop()
        sys.exit(1)
    log(f"[OK  ] Rows gesamt: {total}")
    results.append(True)

    rows = spark.sql(f"""
        SELECT ingestion_id,
               source_version,
               source_file_format,
               source_file_hash,
               source_file_size_bytes,
               raw_payload
        FROM   {TABLE}
        ORDER  BY ingestion_timestamp
    """).collect()

    for idx, row in enumerate(rows, start=1):
        version       = row["source_version"] or "?"
        fmt           = row["source_file_format"]
        expected_hash = row["source_file_hash"]
        expected_size = row["source_file_size_bytes"]
        payload       = row["raw_payload"]

        payload_bytes = payload.encode("utf-8")
        actual_hash   = "sha256:" + hashlib.sha256(payload_bytes).hexdigest()
        actual_size   = len(payload_bytes)

        print(f"\n  --- Row {idx} (source_version={version}) ---", flush=True)

        check("source_file_format", fmt, "csv", results)

        hash_ok = actual_hash == expected_hash
        check("sha256(raw_payload) == source_file_hash", hash_ok, True, results)
        if not hash_ok:
            log(f"       erwartet:  {expected_hash}")
            log(f"       berechnet: {actual_hash}")

        size_ok = actual_size == expected_size
        check("len(raw_payload.encode) == source_file_size_bytes", size_ok, True, results)
        if not size_ok:
            log(f"       source_file_size_bytes: {expected_size}")
            log(f"       len(raw_payload.encode): {actual_size}")

        # CSV-Parsebarkeit + Header-/Datenzeilen-Pruefung
        try:
            reader = csv.reader(io.StringIO(payload))
            rows_parsed = list(reader)
            log(f"[OK  ] csv.reader hat {len(rows_parsed)} Zeilen extrahiert")
            results.append(True)
        except csv.Error as exc:
            log(f"[FAIL] csv.reader fehlgeschlagen: {exc}")
            results.append(False)
            continue

        if not rows_parsed:
            log("[FAIL] Payload enthaelt keine Zeilen")
            results.append(False)
            continue

        actual_header = ",".join(rows_parsed[0])
        check("Header-Zeile identisch", actual_header, EXPECTED_HEADER, results)

        data_rows = len(rows_parsed) - 1
        check("Anzahl Datenzeilen", data_rows, EXPECTED_DATA_ROWS, results)

    passed       = sum(results)
    total_checks = len(results)
    print(f"\n{'=' * 56}", flush=True)
    print(f"  Ergebnis: {passed}/{total_checks} Pruefungen bestanden", flush=True)

    if passed < total_checks:
        print("  FEHLGESCHLAGEN — bitte Ingestion-Logs pruefen", flush=True)
        spark.stop()
        sys.exit(1)
    else:
        print("  ALLE OK", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
