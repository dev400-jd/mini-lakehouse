#!/usr/bin/env bash
# =============================================================================
# demo2-state.sh — State-Machine fuer Demo 2: ESG-Pipeline-Zustaende.
#
# Bringt die ESG-Pipeline in einen definierten Zielzustand. Idempotent:
# jeder Aufruf droppt zuerst alle Demo-2-Tabellen und baut dann auf den
# gewuenschten Zustand neu auf.
#
# Aufruf: ./scripts/demo2-state.sh <state>
#
# State: empty | raw | raw_stg | raw_cur | raw_trusted
#
# WARNUNG: Nur fuer Demo-Zwecke. Droppt Tabellen ohne Backup.
# Demo-1-Pipeline (Fondspreise) bleibt unberuehrt.
# =============================================================================

set -euo pipefail

# Verhindert Git Bash MSYS2-Pfadkonvertierung in docker exec
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
COMPOSE="docker compose"

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

STEP_START=0

step() {
    STEP_START=$(date +%s)
    echo ""
    echo "$(date +%H:%M:%S) [$1] $2"
}

done_step() {
    elapsed=$(( $(date +%s) - STEP_START ))
    echo "$(date +%H:%M:%S)     fertig. (${elapsed}s)"
}

err() {
    echo "$(date +%H:%M:%S) ERROR: $*" >&2
}

print_phase_header() {
    echo ""
    echo "======================================================================"
    echo "  $1"
    echo "======================================================================"
}

usage_and_exit() {
    cat >&2 <<EOF
Usage: $0 <state>

Verfuegbare Zustaende:
  empty         alle Demo-2-Tabellen leer
  raw           nur Raw-Layer befuellt
  raw_stg       Raw + Staging
  raw_cur       Raw + Staging + Curated (Demo-Startpunkt empfohlen)
  raw_trusted   vollstaendige Pipeline (ohne Quality-Gate-Schritt)

Beispiel: $0 raw_cur
EOF
    exit 1
}

trino_exec() {
    $COMPOSE exec -T trino trino --server http://localhost:8080 --execute "$1"
}

# ---------------------------------------------------------------------------
# Voraussetzungs-Check
# ---------------------------------------------------------------------------

check_stack_running() {
    if ! command -v docker > /dev/null 2>&1; then
        err "docker nicht gefunden. Docker Desktop starten."
        exit 2
    fi
    if ! $COMPOSE exec -T trino echo ok > /dev/null 2>&1; then
        err "Trino-Container nicht erreichbar. Stack starten mit:"
        err "  docker compose up -d"
        exit 2
    fi
    if ! $COMPOSE exec -T jupyter echo ok > /dev/null 2>&1; then
        err "jupyter nicht erreichbar."
        exit 2
    fi
}

# ---------------------------------------------------------------------------
# Drop-Logik (umgekehrte Dependency-Reihenfolge)
# ---------------------------------------------------------------------------

drop_all_demo2_tables() {
    step "DROP" "Alle Demo-2-Tabellen entfernen"
    tables=(
        "nessie.trusted.trusted_esg_emissions"
        "nessie.curated.curated_esg_emissions"
        "nessie.curated.curated_companies"
        "nessie.staging.stg_cdp_emissions"
        "nessie.staging.stg_nzdpu_emissions"
        "nessie.raw.cdp_emissions"
        "nessie.raw.nzdpu_emissions"
    )
    for tbl in "${tables[@]}"; do
        echo "  DROP TABLE IF EXISTS ${tbl}"
        trino_exec "DROP TABLE IF EXISTS ${tbl}" > /dev/null 2>&1 || true
    done
    echo "  Alle Demo-2-Tabellen entfernt."
    done_step
}

# ---------------------------------------------------------------------------
# Build-Phasen
# ---------------------------------------------------------------------------

init_raw_tables() {
    step "INIT" "Raw-Tabellen anlegen"
    $COMPOSE exec -T spark-master spark-submit /scripts/init-cdp-table.py
    $COMPOSE exec -T spark-master spark-submit /scripts/init-nzdpu-table.py
    done_step
}

ingest_raw_data() {
    step "INGEST" "CDP und NZDPU laden (deterministische Timestamps)"
    $COMPOSE exec -T spark-master spark-submit \
        /scripts/ingest-cdp.py \
        --file /data/sample/cdp_emissions.csv \
        --ingestion-timestamp 2026-04-20T08:15:00Z

    $COMPOSE exec -T spark-master spark-submit \
        /scripts/ingest-nzdpu.py \
        --file /data/sample/nzdpu_emissions.json \
        --ingestion-timestamp 2026-04-20T08:30:00Z
    done_step
}

build_staging() {
    step "BUILD" "Staging: dbt run stg_cdp_emissions stg_nzdpu_emissions"
    $COMPOSE exec -T jupyter bash -c \
        "cd /home/jovyan/dbt && dbt run --select stg_cdp_emissions stg_nzdpu_emissions"
    done_step
}

build_curated() {
    step "BUILD" "Curated: dbt run curated_companies curated_esg_emissions"
    $COMPOSE exec -T jupyter bash -c \
        "cd /home/jovyan/dbt && dbt run --select curated_companies curated_esg_emissions"
    done_step
}

build_trusted() {
    step "BUILD" "Trusted: dbt run trusted_esg_emissions (ohne Quality Gate)"
    $COMPOSE exec -T jupyter bash -c \
        "cd /home/jovyan/dbt && dbt run --select trusted_esg_emissions"
    done_step
}

# ---------------------------------------------------------------------------
# Zustands-Aufbau
# ---------------------------------------------------------------------------

state_to_empty() {
    drop_all_demo2_tables
}

state_to_raw() {
    drop_all_demo2_tables
    init_raw_tables
    ingest_raw_data
}

state_to_raw_stg() {
    state_to_raw
    build_staging
}

state_to_raw_cur() {
    state_to_raw_stg
    build_curated
}

state_to_raw_trusted() {
    state_to_raw_cur
    build_trusted
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    if [ $# -ne 1 ]; then
        usage_and_exit
    fi

    target_state="$1"

    case "${target_state}" in
        empty|raw|raw_stg|raw_cur|raw_trusted) ;;
        *) usage_and_exit ;;
    esac

    print_phase_header "DEMO 2 STATE: -> ${target_state}"
    echo "  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Repo: ${REPO_ROOT}"

    TOTAL_START=$(date +%s)

    check_stack_running

    case "${target_state}" in
        empty)         state_to_empty ;;
        raw)           state_to_raw ;;
        raw_stg)       state_to_raw_stg ;;
        raw_cur)       state_to_raw_cur ;;
        raw_trusted)   state_to_raw_trusted ;;
    esac

    TOTAL_ELAPSED=$(( $(date +%s) - TOTAL_START ))

    print_phase_header "ZUSTAND ${target_state} ERREICHT (${TOTAL_ELAPSED}s)"
    echo ""
    echo "  Aktueller Stand verifizierbar mit:"
    echo "  ./scripts/demo2-state-verify.sh"
    echo ""
}

main "$@"
