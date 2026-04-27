#!/bin/sh
# =============================================================================
# init-schemas.sh — Legt Nessie-Schemas mit korrekten S3-Bucket-Locations an.
# Wird einmalig vom trino-init Container ausgefuehrt.
# depends_on: service_healthy stellt sicher, dass Trino bereits bereit ist.
# =============================================================================

TRINO_HOST="trino"
TRINO_PORT="8080"

run_sql() {
    trino --server "http://${TRINO_HOST}:${TRINO_PORT}" \
          --user init \
          --execute "$1"
}

echo ">>> Lege Nessie-Schemas mit Bucket-Locations an..."

run_sql "CREATE SCHEMA IF NOT EXISTS nessie.staging WITH (location = 's3a://staging/')"
echo ">>> Schema staging -> s3a://staging/"

run_sql "CREATE SCHEMA IF NOT EXISTS nessie.curated WITH (location = 's3a://curated/')"
echo ">>> Schema curated -> s3a://curated/"

run_sql "CREATE SCHEMA IF NOT EXISTS nessie.trusted WITH (location = 's3a://trusted/')"
echo ">>> Schema trusted -> s3a://trusted/"

echo ">>> Alle Schemas angelegt."
