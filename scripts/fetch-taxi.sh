#!/usr/bin/env bash
# =============================================================================
# fetch-taxi.sh — Laedt N Monate NYC-TLC Yellow-Taxi-Parquet-Dateien fuer DE1
# (Engine-Right-Sizing fuer Ingestion).
#
# Bewusst NICHT im Git (siehe .gitignore: data/taxi/) — jede Gruppe bezieht
# die Dateien selbst, am besten schon am Pre-Flight-Abend (siehe Briefing).
#
# Nutzung:
#   make fetch-taxi N=3          # ueber Makefile (empfohlen)
#   bash scripts/fetch-taxi.sh 3 # direkt
#
# Laedt die ersten N Monate ab Januar 2024 (2024-01, 2024-02, ...). Ueberspringt
# bereits vorhandene Dateien, damit wiederholte Aufrufe guenstig sind.
# =============================================================================

set -euo pipefail

N="${1:-1}"
START_YEAR=2024
START_MONTH=1
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/data/taxi"

if ! [[ "$N" =~ ^[0-9]+$ ]] || [ "$N" -lt 1 ]; then
  echo "Fehler: N muss eine positive Zahl sein (z.B. make fetch-taxi N=3), erhalten: '$N'" >&2
  exit 1
fi

mkdir -p "$DEST_DIR"

echo "═══════════════════════════════════════════"
echo "  fetch-taxi — NYC TLC Yellow-Taxi-Daten"
echo "═══════════════════════════════════════════"
echo "  Monate: $N (ab ${START_YEAR}-$(printf '%02d' "$START_MONTH"))"
echo "  Ziel:   $DEST_DIR"
echo ""

year=$START_YEAR
month=$START_MONTH

for i in $(seq 1 "$N"); do
  ym=$(printf "%04d-%02d" "$year" "$month")
  file="yellow_tripdata_${ym}.parquet"
  dest="${DEST_DIR}/${file}"

  if [ -f "$dest" ]; then
    size=$(du -h "$dest" | cut -f1)
    echo "  [skip]  $file bereits vorhanden ($size)"
  else
    echo "  [fetch] $file ..."
    if curl -fL --progress-bar -o "${dest}.tmp" "${BASE_URL}/${file}"; then
      mv "${dest}.tmp" "$dest"
      size=$(du -h "$dest" | cut -f1)
      echo "  [ok]    $file geladen ($size)"
    else
      rm -f "${dest}.tmp"
      echo "  [FEHLER] $file konnte nicht geladen werden (Monat evtl. noch nicht veroeffentlicht?)" >&2
      exit 1
    fi
  fi

  month=$((month + 1))
  if [ "$month" -gt 12 ]; then
    month=1
    year=$((year + 1))
  fi
done

echo ""
echo "═══════════════════════════════════════════"
echo "  Fertig. Dateien in ${DEST_DIR}:"
echo "───────────────────────────────────────────"
ls -lh "$DEST_DIR"/*.parquet 2>/dev/null | awk '{printf "  %-40s %s\n", $NF, $5}'
echo "═══════════════════════════════════════════"
