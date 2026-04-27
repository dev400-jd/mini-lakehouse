#!/usr/bin/env bash
# =============================================================================
# demo2-state-verify.sh — Zeigt Row-Counts der Demo-2-Pipeline.
#
# Hilfsskript fuer ./demo2-state.sh: gibt fuer jede Demo-2-Tabelle die
# aktuelle Zeilenanzahl aus. Bei nicht-existenten Tabellen "— rows".
# =============================================================================

set -uo pipefail

export MSYS_NO_PATHCONV=1

COMPOSE="docker compose"

echo ""
echo "Demo-2-Pipeline Status:"

tables=(
    "raw.cdp_emissions"
    "raw.nzdpu_emissions"
    "staging.stg_cdp_emissions"
    "staging.stg_nzdpu_emissions"
    "curated.curated_companies"
    "curated.curated_esg_emissions"
    "trusted.trusted_esg_emissions"
)

for tbl in "${tables[@]}"; do
    raw_out=$($COMPOSE exec -T trino trino --server http://localhost:8080 \
        --execute "SELECT COUNT(*) FROM nessie.${tbl}" 2>/dev/null || true)
    count=$(echo "$raw_out" | grep -E '^"?[0-9]+"?$' | tail -1 | tr -d '"')
    if [ -z "${count}" ]; then
        count="—"
    fi
    printf "  %-45s %s rows\n" "${tbl}" "${count}"
done
echo ""
