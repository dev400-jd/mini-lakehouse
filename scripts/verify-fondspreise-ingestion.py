"""
verify-fondspreise-ingestion.py — Prueft raw.fondspreise nach beiden Ingestion-Laeufen.

Erwartet nach Load 1 + Load 2 (Variante B — File-level Payload):
  - Rows gesamt:            2
  - DISTINCT ingestion_id:  2
  - Iceberg-Snapshots:      2
  - sha256(raw_payload) == source_file_hash fuer jeden Row
  - len(raw_payload.encode("utf-8")) == source_file_size_bytes fuer jeden Row
  - json.loads(raw_payload)["records"] hat 450 Elemente (Load 1) bzw. 1 (Load 2)

Ausfuehrung:
  docker compose exec spark-master spark-submit /scripts/verify-fondspreise-ingestion.py
"""

import hashlib
import json
import sys

from pyspark.sql import SparkSession

TABLE = "nessie.raw.fondspreise"

EXPECTED_ROWS  = 2
EXPECTED_RUNS  = 2
EXPECTED_SNAPS = 2


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def check(label: str, actual, expected, results: list) -> None:
    ok = actual == expected
    status = "OK  " if ok else "FAIL"
    log(f"[{status}] {label}: {actual} (erwartet: {expected})")
    results.append(ok)


def main() -> None:
    spark = SparkSession.builder.appName("verify-fondspreise").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    print(f"\nVerifikation: {TABLE}", flush=True)
    print("=" * 56, flush=True)

    results = []

    # 1. Gesamt-Rows (2 = ein Row pro File)
    total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
    check("Rows gesamt", total, EXPECTED_ROWS, results)

    # 2. Verschiedene Ingestion-Runs
    runs = spark.sql(
        f"SELECT COUNT(DISTINCT ingestion_id) AS cnt FROM {TABLE}"
    ).collect()[0]["cnt"]
    check("DISTINCT ingestion_id", runs, EXPECTED_RUNS, results)

    # 3. Iceberg-Snapshots
    snaps = (
        spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.snapshots")
        .collect()[0]["cnt"]
    )
    check("Iceberg Snapshots", snaps, EXPECTED_SNAPS, results)

    # 4. Pro-Row-Checks: Hash, Size, JSON-Struktur
    print("\n  Pro-Row-Verifikation:", flush=True)
    rows = spark.sql(f"""
        SELECT source_version, source_file_hash, source_file_size_bytes, raw_payload
        FROM   {TABLE}
        ORDER  BY ingestion_timestamp
    """).collect()

    if not rows:
        log("[FAIL] Keine Rows gefunden — Ingestion fehlgeschlagen?")
        results.append(False)
    else:
        for row in rows:
            version   = row["source_version"] or "?"
            expected_hash = row["source_file_hash"]
            expected_size = row["source_file_size_bytes"]
            payload   = row["raw_payload"]

            payload_bytes = payload.encode("utf-8")
            actual_hash   = "sha256:" + hashlib.sha256(payload_bytes).hexdigest()
            actual_size   = len(payload_bytes)

            print(f"\n  --- source_version={version} ---", flush=True)

            hash_ok = actual_hash == expected_hash
            check(f"  sha256 stimmt ueberein", hash_ok, True, results)
            if not hash_ok:
                log(f"       erwartet: {expected_hash}")
                log(f"       berechnet: {actual_hash}")

            size_ok = actual_size == expected_size
            check(f"  Dateigroesse stimmt ueberein", size_ok, True, results)
            if not size_ok:
                log(f"       source_file_size_bytes: {expected_size}")
                log(f"       len(raw_payload.encode): {actual_size}")

            try:
                parsed   = json.loads(payload)
                rec_list = parsed.get("records", [])
                log(f"[OK  ] json.loads(raw_payload) erfolgreich")
                log(f"       records-Anzahl: {len(rec_list)}")
                results.append(True)
            except json.JSONDecodeError as exc:
                log(f"[FAIL] json.loads(raw_payload) fehlgeschlagen: {exc}")
                results.append(False)

    # Zusammenfassung
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
