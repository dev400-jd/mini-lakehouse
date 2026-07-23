#!/usr/bin/env bash
# =============================================================================
# fetch-taxi.sh — Laedt N Monate NYC-TLC Yellow-Taxi-Parquet-Dateien fuer DE1
# (Engine-Right-Sizing fuer Ingestion).
#
# Bewusst NICHT im Git (siehe .gitignore: data/taxi/) — jede Gruppe bezieht
# die Dateien selbst, am besten schon am Pre-Flight-Abend (siehe Briefing).
#
# Nutzung:
#   make fetch-taxi N=3                     # ueber Makefile (empfohlen), ab 2024-01
#   make fetch-taxi FROM=2009-01 N=195       # ab beliebigem Startmonat (z.B. volle Historie)
#   bash scripts/fetch-taxi.sh 3 2024-01     # direkt
#
# Laedt die ersten N Monate ab FROM (Default 2024-01). Ueberspringt bereits
# vorhandene Dateien, damit wiederholte Aufrufe guenstig sind. Yellow-Taxi-Daten
# sind ab 2009-01 verfuegbar; die volle Historie (2009-01 bis heute) summiert
# sich auf ca. 50GB.
# =============================================================================

set -euo pipefail

N="${1:-1}"
FROM="${2:-2024-01}"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/data/taxi"

if ! [[ "$N" =~ ^[0-9]+$ ]] || [ "$N" -lt 1 ]; then
  echo "Fehler: N muss eine positive Zahl sein (z.B. make fetch-taxi N=3), erhalten: '$N'" >&2
  exit 1
fi

if ! [[ "$FROM" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
  echo "Fehler: FROM muss im Format YYYY-MM sein (z.B. FROM=2009-01), erhalten: '$FROM'" >&2
  exit 1
fi

START_YEAR="${FROM%%-*}"
START_MONTH="${FROM##*-}"
START_MONTH=$((10#$START_MONTH))

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
