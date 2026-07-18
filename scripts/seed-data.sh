#!/usr/bin/env bash
# seed-data.sh — Automatisierter Daten-Setup: Beispieldaten -> Iceberg Raw Layer
#
# Aufruf: make seed  (oder direkt: bash scripts/seed-data.sh)
# Idempotent: mehrfach ausführbar ohne Fehler oder Duplikate.

set -e

# ---------------------------------------------------------------------------
# Farben & Hilfsfunktionen
# ---------------------------------------------------------------------------

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
RESET="\033[0m"
BOLD="\033[1m"

ok()   { echo -e "  ${GREEN}✅${RESET} $*"; }
fail() { echo -e "  ${RED}❌  $*${RESET}"; }
info() { echo -e "  ${YELLOW}ℹ${RESET}  $*"; }

# Skript-Verzeichnis ermitteln — funktioniert auch bei symlinks und "make seed"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_SAMPLE_DIR="${REPO_ROOT}/data/sample"

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

echo ""
echo -e "${BOLD}═══════════════════════════════════════${RESET}"
echo -e "${BOLD}   Mini-Lakehouse — Daten-Setup${RESET}"
echo -e "${BOLD}═══════════════════════════════════════${RESET}"
echo ""

# ---------------------------------------------------------------------------
# [1/4] Services prüfen
# ---------------------------------------------------------------------------

echo -e "${BOLD}[1/4] Services prüfen...${RESET}"

check_service() {
    local name="$1"
    local status
    status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "missing")
    if [ "$status" != "healthy" ]; then
        fail "Service ${name} ist nicht healthy (Status: ${status})"
        return 1
    fi
    return 0
}

SERVICE_OK=true
for svc in minio postgres nessie spark-master; do
    if ! check_service "$svc"; then
        SERVICE_OK=false
    fi
done

if [ "$SERVICE_OK" = false ]; then
    echo ""
    echo -e "${RED}${BOLD}Services nicht gestartet. Bitte zuerst: make up${RESET}"
    echo ""
    exit 1
fi

ok "Alle Services healthy (minio, postgres, nessie, spark-master)"

# ---------------------------------------------------------------------------
# [2/4] Beispieldaten prüfen / generieren
# ---------------------------------------------------------------------------

echo ""
echo -e "${BOLD}[2/4] Beispieldaten prüfen...${RESET}"

REQUIRED_FILES=(
    "nzdpu_emissions.json"
    "cdp_emissions.csv"
    "owid_co2_countries.csv"
    "fund_master.csv"
    "fund_positions.csv"
)

files_missing=false
for f in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "${DATA_SAMPLE_DIR}/${f}" ]; then
        files_missing=true
        break
    fi
done

if [ "$files_missing" = false ]; then
    ok "Beispieldaten bereits vorhanden — überspringe Generierung"
else
    info "Dateien fehlen — generiere Beispieldaten ..."
    cd "${REPO_ROOT}"

    if command -v uv &>/dev/null; then
        uv run scripts/generate-sample-data.py
    elif command -v python3 &>/dev/null; then
        info "uv nicht gefunden — nutze python3 als Fallback"
        python3 scripts/generate-sample-data.py
    else
        fail "Weder uv noch python3 gefunden"
        exit 1
    fi

    # Ergebnis prüfen
    for f in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "${DATA_SAMPLE_DIR}/${f}" ]; then
            fail "Datei nach Generierung nicht vorhanden: ${f}"
            exit 1
        fi
    done
    ok "Beispieldaten generiert (${DATA_SAMPLE_DIR})"
fi

# ---------------------------------------------------------------------------
# [3/4] Spark Ingestion
# ---------------------------------------------------------------------------

echo ""
echo -e "${BOLD}[3/4] Spark Ingestion...${RESET}"
info "Starte spark-submit (dauert ~2-3 Minuten) ..."

INGESTION_START=$(date +%s)

# spark_submit <beschreibung> <spark-submit-argumente...>
# Fuehrt einen spark-submit im spark-master aus, bricht bei Fehler mit den
# letzten 20 Ausgabezeilen ab.
spark_submit() {
    local desc="$1"; shift
    local out
    out=$(
        MSYS_NO_PATHCONV=1 docker compose exec -T spark-master \
            /opt/spark/bin/spark-submit --master spark://spark-master:7077 "$@" 2>&1
    ) || {
        fail "Spark Ingestion fehlgeschlagen: ${desc}"
        echo ""
        echo "--- Letzten 20 Zeilen der Ausgabe ---"
        echo "$out" | tail -20
        exit 1
    }
}

# Geparste Referenztabellen: owid_co2_countries, fund_master, fund_positions
spark_submit "geparste Referenztabellen" /scripts/spark-ingestion.py

# File-level Raw (raw_payload) fuer die dbt-ESG-Pipeline: cdp + nzdpu.
# Deterministische Ingestion-Timestamps wie in demo2-state.sh, damit der
# Seed reproduzierbar bleibt.
spark_submit "cdp init"  /scripts/init-cdp-table.py
spark_submit "cdp ingest" \
    /scripts/ingest-cdp.py \
    --file /data/sample/cdp_emissions.csv \
    --ingestion-timestamp 2026-04-20T08:15:00Z

spark_submit "nzdpu init" /scripts/init-nzdpu-table.py
spark_submit "nzdpu ingest" \
    /scripts/ingest-nzdpu.py \
    --file /data/sample/nzdpu_emissions.json \
    --ingestion-timestamp 2026-04-20T08:30:00Z

INGESTION_END=$(date +%s)
INGESTION_SECS=$((INGESTION_END - INGESTION_START))

ok "Spark Ingestion erfolgreich (${INGESTION_SECS}s)"

# ---------------------------------------------------------------------------
# Nessie Namespaces für dbt anlegen (idempotent)
# ---------------------------------------------------------------------------

info "Lege Nessie-Schemas mit Bucket-Locations an (staging, curated) ..."
MSYS_NO_PATHCONV=1 docker compose exec -T trino \
    trino --execute "CREATE SCHEMA IF NOT EXISTS nessie.staging WITH (location = 's3a://staging/')" > /dev/null 2>&1
MSYS_NO_PATHCONV=1 docker compose exec -T trino \
    trino --execute "CREATE SCHEMA IF NOT EXISTS nessie.curated WITH (location = 's3a://curated/')" > /dev/null 2>&1
ok "Schemas staging (s3a://staging/) + curated (s3a://curated/) bereit"

# ---------------------------------------------------------------------------
# [4/4] Verifizierung via Trino
# ---------------------------------------------------------------------------

echo ""
echo -e "${BOLD}[4/4] Verifizierung via Trino...${RESET}"

# Tabellen auflisten
TABLES_OUTPUT=$(
    MSYS_NO_PATHCONV=1 docker compose exec -T trino \
        trino --execute "SHOW TABLES IN nessie.raw" 2>/dev/null
) || {
    fail "Trino-Abfrage fehlgeschlagen"
    exit 1
}

TABLE_COUNT=$(echo "$TABLES_OUTPUT" | grep -c '^\S' || true)

if [ "$TABLE_COUNT" -lt 5 ]; then
    fail "Nur ${TABLE_COUNT} Tabellen gefunden (erwartet: 5)"
    echo "$TABLES_OUTPUT"
    exit 1
fi

ok "Verifizierung erfolgreich (${TABLE_COUNT} Tabellen in nessie.raw)"

# ---------------------------------------------------------------------------
# Zusammenfassung
# ---------------------------------------------------------------------------

echo ""
echo -e "${BOLD}═══════════════════════════════════════${RESET}"
echo -e "${BOLD}   Raw Layer Tabellen${RESET}"
echo -e "${BOLD}───────────────────────────────────────${RESET}"

declare -A TABLE_LABELS=(
    ["nzdpu_emissions"]="nzdpu_emissions"
    ["cdp_emissions"]="cdp_emissions"
    ["owid_co2_countries"]="owid_co2_countries"
    ["fund_master"]="fund_master"
    ["fund_positions"]="fund_positions"
)

VERIFY_FAILED=false
for tbl in nzdpu_emissions cdp_emissions owid_co2_countries fund_master fund_positions; do
    CNT=$(
        MSYS_NO_PATHCONV=1 docker compose exec -T trino \
            trino --execute "SELECT count(*) FROM nessie.raw.${tbl}" 2>/dev/null \
        | tr -d ' "' | grep '^[0-9]' || echo "?"
    )
    if [ "$CNT" = "?" ]; then
        VERIFY_FAILED=true
        printf "  ${RED}%-25s  ???${RESET}\n" "${tbl}"
    else
        printf "  ${GREEN}%-25s  %s Zeilen${RESET}\n" "${tbl}" "${CNT}"
    fi
done

echo -e "${BOLD}═══════════════════════════════════════${RESET}"
echo ""

if [ "$VERIFY_FAILED" = true ]; then
    fail "Einige Tabellen konnten nicht verifiziert werden"
    exit 1
fi

echo -e "${GREEN}${BOLD}Raw Layer bereit. Weiter mit: make demo${RESET}"
echo ""
