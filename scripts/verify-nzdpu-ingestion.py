"""
verify-nzdpu-ingestion.py — Selbstpruefung von raw.nzdpu_emissions nach einem Ingest.

Sieben Checks pro Row (gemaess AP-9 DoD):
  1. Rows gesamt >= 1
  2. source_file_format == "json"
  3. sha256(raw_payload.encode()) == source_file_hash
  4. len(raw_payload.encode("utf-8")) == source_file_size_bytes
  5. json.loads(raw_payload) liefert Wrapper-Objekt mit
     {status, total_records, source, data}
  6. len(parsed["data"]) == 30 und parsed["data"][0]["reporting_periods"]
     hat 3 Eintraege
  7. Summe aller reporting_periods ueber alle 30 Companies == 90

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/verify-nzdpu-ingestion.py
"""

import hashlib
import json
import sys

from pyspark.sql import SparkSession

TABLE = "nessie.raw.nzdpu_emissions"

EXPECTED_WRAPPER_KEYS = {"status", "total_records", "source", "data"}
EXPECTED_COMPANIES = 30
EXPECTED_PERIODS_FIRST = 3
EXPECTED_PERIODS_TOTAL = 90


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def check(label: str, actual, expected, results: list) -> None:
    ok = actual == expected
    status = "OK  " if ok else "FAIL"
    log(f"[{status}] {label}: {actual} (erwartet: {expected})")
    results.append(ok)


def main() -> None:
    spark = SparkSession.builder.appName("verify-nzdpu").getOrCreate()
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
               source_system,
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
        system        = row["source_system"]
        fmt           = row["source_file_format"]
        expected_hash = row["source_file_hash"]
        expected_size = row["source_file_size_bytes"]
        payload       = row["raw_payload"]

        payload_bytes = payload.encode("utf-8")
        actual_hash   = "sha256:" + hashlib.sha256(payload_bytes).hexdigest()
        actual_size   = len(payload_bytes)

        print(f"\n  --- Row {idx} (source_system={system}, version={version}) ---",
              flush=True)

        check("source_file_format", fmt, "json", results)

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

        # JSON-Parse + Wrapper-Schluessel
        try:
            parsed = json.loads(payload)
            log("[OK  ] json.loads(raw_payload) erfolgreich")
            results.append(True)
        except json.JSONDecodeError as exc:
            log(f"[FAIL] json.loads(raw_payload) fehlgeschlagen: {exc}")
            results.append(False)
            continue

        if not isinstance(parsed, dict):
            log(f"[FAIL] Top-Level ist {type(parsed).__name__}, erwartet dict")
            results.append(False)
            continue

        wrapper_keys = set(parsed.keys())
        missing = EXPECTED_WRAPPER_KEYS - wrapper_keys
        if missing:
            log(f"[FAIL] Fehlende Wrapper-Schluessel: {sorted(missing)}")
            results.append(False)
        else:
            log(f"[OK  ] Wrapper-Schluessel vorhanden: "
                f"{sorted(EXPECTED_WRAPPER_KEYS)}")
            results.append(True)

        data = parsed.get("data") or []
        check("Anzahl Companies in data[]", len(data), EXPECTED_COMPANIES, results)

        if data:
            first_periods = data[0].get("reporting_periods") or []
            check(
                "reporting_periods im ersten Company",
                len(first_periods),
                EXPECTED_PERIODS_FIRST,
                results,
            )

            total_periods = sum(
                len(c.get("reporting_periods") or []) for c in data
            )
            check(
                "Summe aller reporting_periods (Companies × Jahre)",
                total_periods,
                EXPECTED_PERIODS_TOTAL,
                results,
            )

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
