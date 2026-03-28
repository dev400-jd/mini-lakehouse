#!/bin/bash
# =============================================================================
# before-spark-conf.sh — Wird von jupyter-docker-stacks vor dem Notebook-Start
# ausgeführt (before-notebook.d Hook).
#
# Ersetzt ${MINIO_ROOT_USER} und ${MINIO_ROOT_PASSWORD} in
# spark-defaults.conf.template und schreibt das Ergebnis nach
# SPARK_CONF_DIR (/home/jovyan/spark-conf/spark-defaults.conf).
#
# Gleicher Ansatz wie config/spark/entrypoint.sh im Spark-Image.
# =============================================================================

set -e

envsubst < /home/jovyan/spark-defaults.conf.template \
         > "${SPARK_CONF_DIR}/spark-defaults.conf"
