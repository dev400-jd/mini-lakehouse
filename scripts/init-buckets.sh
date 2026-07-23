#!/bin/sh
# =============================================================================
# init-buckets.sh — Legt die S3-Buckets in MinIO an.
# Wird einmalig vom minio-init Container ausgeführt.
# depends_on: service_healthy stellt sicher, dass MinIO bereits bereit ist.
# =============================================================================

MINIO_ENDPOINT="http://minio:9000"
ALIAS="lakehouse"

echo ">>> Setze mc alias..."
mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

for BUCKET in "${S3_BUCKET_RAW}" "${S3_BUCKET_STAGING}" "${S3_BUCKET_CURATED}" "${S3_BUCKET_TRUSTED}" "${S3_BUCKET_WAREHOUSE}"; do
  mc mb --ignore-existing "${ALIAS}/${BUCKET}"
  echo ">>> Bucket bereit: ${BUCKET}"
done

echo ">>> Alle Buckets angelegt."
