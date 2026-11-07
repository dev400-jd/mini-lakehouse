#!/usr/bin/env bash
# =============================================================================
# reset-mlflow-demo.sh — Setzt MLflow auf einen sauberen Stand fuer die Live-Demo.
#
# Loescht ALLE Runs, Experimente und Modelle in MLflow (NUR MLflow, nicht das
# Lakehouse): DROP+CREATE der Postgres-DB "mlflow" + Leeren des MinIO-Buckets
# "mlflow". Danach erzeugt genau ein `make train` einen sauberen Stand
# (6 Runs + 1 Modellversion in der Registry).
#
# Aufruf (Repo-Root):  bash scripts/reset-mlflow-demo.sh
# Idempotent: beliebig oft ausfuehrbar.
# =============================================================================
set -e

GREEN="\033[0;32m"; YELLOW="\033[0;33m"; RESET="\033[0m"
info() { echo -e "  ${YELLOW}>>>${RESET} $*"; }
ok()   { echo -e "  ${GREEN}OK${RESET}  $*"; }

NET="$(docker network ls --format '{{.Name}}' | grep lakehouse-net | head -1)"

info "Stoppe mlflow-Service ..."
docker compose stop mlflow >/dev/null

info "Setze Postgres-DB 'mlflow' zurueck (DROP + CREATE) ..."
docker exec postgres psql -U lakehouse -d nessie -v ON_ERROR_STOP=1 \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='mlflow';" \
  -c "DROP DATABASE IF EXISTS mlflow;" \
  -c "CREATE DATABASE mlflow;" >/dev/null
ok "DB 'mlflow' ist leer."

info "Leere MinIO-Bucket 'mlflow' ..."
docker run --rm --network "$NET" \
  -e MC_HOST_h="http://lakehouse:lakehouse123@minio:9000" \
  minio/mc rm --recursive --force h/mlflow/ >/dev/null 2>&1 || true
docker run --rm --network "$NET" \
  -e MC_HOST_h="http://lakehouse:lakehouse123@minio:9000" \
  minio/mc mb --ignore-existing h/mlflow >/dev/null 2>&1 || true
ok "Bucket 'mlflow' ist leer."

info "Starte mlflow-Service ..."
docker compose start mlflow >/dev/null

info "Warte auf MLflow-Health ..."
for i in $(seq 1 20); do
  if docker exec mlflow curl -sf http://localhost:5000/health >/dev/null 2>&1; then
    ok "MLflow ist wieder erreichbar."
    echo -e "\n  ${GREEN}Sauberer Stand hergestellt.${RESET} Weiter mit: ${YELLOW}make train${RESET}\n"
    exit 0
  fi
  sleep 2
done
echo "  MLflow wurde gestartet, Health-Check noch offen — kurz warten und pruefen."
