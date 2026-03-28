#!/bin/bash
# =============================================================================
# init-databases.sh — Legt zusätzliche PostgreSQL-Datenbanken an.
# Wird einmalig beim ersten Start des Containers ausgeführt
# (Docker-Mechanismus: /docker-entrypoint-initdb.d/).
# Die Datenbank "nessie" wurde bereits via POSTGRES_DB angelegt.
# =============================================================================

set -e

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "nessie" <<-EOSQL
    CREATE DATABASE dagster;
EOSQL

echo ">>> Datenbank 'dagster' angelegt."
