#!/bin/bash
# =============================================================================
# Spark Custom Entrypoint
# Ersetzt Platzhalter in spark-defaults.conf.template mit Werten aus
# Umgebungsvariablen (MINIO_ROOT_USER, MINIO_ROOT_PASSWORD), dann
# delegiert an den originalen Spark-Entrypoint.
# =============================================================================

set -e

# SPARK_CONF_DIR ist als Docker-ENV gesetzt (/opt/spark/custom-conf).
# Das gilt für alle Prozesse im Container inkl. spark-submit via docker exec.
envsubst < /opt/spark/conf/spark-defaults.conf.template \
         > "${SPARK_CONF_DIR}/spark-defaults.conf"

exec /opt/entrypoint.sh "$@"
