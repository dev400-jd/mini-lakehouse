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

set -euo pipefail

echo "Creating additional databases and users..."

# ab hier beginnt Superset

set -euo pipefail

echo "Creating additional databases and users..."

psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --set=ON_ERROR_STOP=1 <<-EOSQL

DO
\$\$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE rolname = '${SUPERSET_DB_USER}'
    ) THEN
        CREATE ROLE ${SUPERSET_DB_USER}
            LOGIN
            PASSWORD '${SUPERSET_DB_PASSWORD}';
    END IF;
END
\$\$;

SELECT 'CREATE DATABASE superset OWNER ${SUPERSET_DB_USER}'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'superset'
)
\gexec

GRANT ALL PRIVILEGES
ON DATABASE superset
TO ${SUPERSET_DB_USER};

EOSQL

echo "PostgreSQL initialization completed."
