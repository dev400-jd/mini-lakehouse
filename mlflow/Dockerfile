# =============================================================================
# MLflow Tracking Server
#
# Backend-Store:  PostgreSQL (DB "mlflow")  -> psycopg2-binary
# Artifact-Store: MinIO (S3-kompatibel)     -> boto3
#
# Der Server laeuft mit --serve-artifacts: Clients (Jupyter) laden Artefakte
# ueber den Tracking-Server hoch/runter und brauchen selbst KEINE S3-Credentials.
# =============================================================================

ARG MLFLOW_VERSION=2.19.0
FROM python:3.12-slim

ARG MLFLOW_VERSION
RUN pip install --no-cache-dir \
      "mlflow==${MLFLOW_VERSION}" \
      "psycopg2-binary>=2.9" \
      "boto3>=1.34"

# curl fuer den Healthcheck
RUN apt-get update -q && \
    apt-get install -y -q --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 5000
