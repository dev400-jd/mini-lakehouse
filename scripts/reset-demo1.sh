#!/bin/sh
# =============================================================================
# reset-demo1.sh — Stellt konsistenten Startzustand fuer Demo 1 her.
#
# WARNUNG: Nur fuer Demo-Zwecke. Droppt Tabellen ohne Backup und
# regeneriert synthetische Daten. NIEMALS in Produktion ausfuehren.
#
# Zielzustand nach Ausfuehrung:
#   raw.fondspreise:              1 Row (Load-1-Payload)
#   staging.stg_fondspreise:      450 Zeilen
#   curated.snp_fondspreise_scd2: 450 Zeilen, alle dbt_valid_to = NULL
#
# Load 2 wird NICHT ausgefuehrt — das ist der Live-Demo-Moment.
# ESG-Tabellen (raw.*_emissions, staging.stg_*) bleiben unveraendert.
# =============================================================================

set -eu

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

STEP_START=0

step() {
    STEP_START=$SECONDS
    echo ""
    echo "$(date +%H:%M:%S) [$1] $2"
}

done_step() {
    local elapsed=$(( SECONDS - STEP_START ))
    echo "$(date +%H:%M:%S)     fertig. (${elapsed}s)"
}

err() {
    echo "$(date +%H:%M:%S) ERROR: $*" >&2
}

# ---------------------------------------------------------------------------
# Repo-Root als Arbeitsverzeichnis (unabhaengig vom Aufruf-Pfad)
# ---------------------------------------------------------------------------

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
COMPOSE="docker compose"

echo ""
echo "============================================"
echo "  Demo 1 Reset"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Repo: $REPO_ROOT"
echo "============================================"

TOTAL_START=$SECONDS

# ---------------------------------------------------------------------------
# Schritt 0: Voraussetzungs-Check
# ---------------------------------------------------------------------------

step "0/5" "Voraussetzungs-Check..."

if ! command -v docker &> /dev/null; then
    err "docker nicht gefunden. Docker Desktop starten."
    exit 1
fi

if ! $COMPOSE exec -T trino trino --execute "SELECT 1" > /dev/null 2>&1; then
    err "Trino nicht erreichbar. Stack starten mit:"
    err "  docker compose up -d"
    exit 1
fi

if ! $COMPOSE exec -T spark-master echo ok > /dev/null 2>&1; then
    err "spark-master nicht erreichbar."
    exit 1
fi

if ! $COMPOSE exec -T jupyter echo ok > /dev/null 2>&1; then
    err "jupyter nicht erreichbar."
    exit 1
fi

echo "  Stack laeuft, alle Services erreichbar."
done_step

# ---------------------------------------------------------------------------
# Schritt 1: Tabellen droppen (umgekehrte Dependency-Reihenfolge)
# ---------------------------------------------------------------------------

step "1/5" "Droppe Fondspreis-Tabellen..."

$COMPOSE exec -T trino trino --execute \
    "DROP TABLE IF EXISTS nessie.curated.snp_fondspreise_scd2"
echo "  curated.snp_fondspreise_scd2 gedroppt (oder war nicht vorhanden)"

$COMPOSE exec -T trino trino --execute \
    "DROP TABLE IF EXISTS nessie.staging.stg_fondspreise"
echo "  staging.stg_fondspreise gedroppt (oder war nicht vorhanden)"

$COMPOSE exec -T trino trino --execute \
    "DROP TABLE IF EXISTS nessie.raw.fondspreise"
echo "  raw.fondspreise gedroppt (oder war nicht vorhanden)"

# Schemas sicherstellen (idempotent, mit S3-Locations)
$COMPOSE exec -T trino trino --execute \
    "CREATE SCHEMA IF NOT EXISTS nessie.raw WITH (location = 's3a://raw/')"
$COMPOSE exec -T trino trino --execute \
    "CREATE SCHEMA IF NOT EXISTS nessie.staging WITH (location = 's3a://staging/')"
$COMPOSE exec -T trino trino --execute \
    "CREATE SCHEMA IF NOT EXISTS nessie.curated WITH (location = 's3a://curated/')"
echo "  Schemas raw/staging/curated sichergestellt."

done_step

# ---------------------------------------------------------------------------
# Schritt 2: Demo-Daten generieren
# ---------------------------------------------------------------------------

step "2/5" "Generiere Demo-Daten (Seed: 20260422)..."

uv run python scripts/generate-fondspreise.py

done_step

# ---------------------------------------------------------------------------
# Schritt 3: Load 1 via Spark ingestieren
# ---------------------------------------------------------------------------

step "3/5" "Ingest Load 1 -> nessie.raw.fondspreise..."

$COMPOSE exec -T spark-master spark-submit \
    /scripts/ingest-fondspreise.py \
    --file /data/sample/fondspreise_load1.json \
    --ingestion-timestamp 2026-04-20T08:15:00Z

done_step

# ---------------------------------------------------------------------------
# Schritt 4: Staging via dbt
# ---------------------------------------------------------------------------

step "4/5" "Staging: dbt run stg_fondspreise..."

$COMPOSE exec -T jupyter bash -c \
    "cd /home/jovyan/dbt && dbt run --select stg_fondspreise"

done_step

# ---------------------------------------------------------------------------
# Schritt 5: Snapshot Erstrun via dbt
# ---------------------------------------------------------------------------

step "5/5" "Snapshot Erstrun: dbt snapshot snp_fondspreise_scd2..."

$COMPOSE exec -T jupyter bash -c \
    "cd /home/jovyan/dbt && dbt snapshot --select snp_fondspreise_scd2"

done_step

# ---------------------------------------------------------------------------
# Zusammenfassung
# ---------------------------------------------------------------------------

TOTAL_ELAPSED=$(( SECONDS - TOTAL_START ))

echo ""
echo "============================================"
echo "  Reset abgeschlossen in ${TOTAL_ELAPSED}s"
echo "============================================"
echo ""
echo "  raw.fondspreise:              1 Row (Load-1-Datei, 450 Records als Payload)"
echo "  staging.stg_fondspreise:      450 Zeilen"
echo "  curated.snp_fondspreise_scd2: 450 Zeilen, alle dbt_valid_to = NULL"
echo ""
echo "  Bereit fuer Demo 1."
echo "  Load 2 wird LIVE in der Demo ausgefuehrt."
echo ""
