#!/bin/bash
# =============================================================================
# init-openmetadata-db.sh — Legt die Postgres-Datenbank fuer OpenMetadata an,
# falls sie noch nicht existiert (idempotent).
# Wird einmalig vom openmetadata-db-init Container ausgefuehrt.
# depends_on: service_healthy stellt sicher, dass Postgres bereits bereit ist.
# =============================================================================

set -e

DB_EXISTS=$(PGPASSWORD="${POSTGRES_PASSWORD}" psql -h postgres -U "${POSTGRES_USER}" -d nessie -tAc \
    "SELECT 1 FROM pg_database WHERE datname='${OPENMETADATA_DB}'")

if [ "${DB_EXISTS}" = "1" ]; then
    echo ">>> Datenbank '${OPENMETADATA_DB}' existiert bereits."
else
    PGPASSWORD="${POSTGRES_PASSWORD}" psql -h postgres -U "${POSTGRES_USER}" -d nessie -c \
        "CREATE DATABASE ${OPENMETADATA_DB};"
    echo ">>> Datenbank '${OPENMETADATA_DB}' angelegt."
fi
