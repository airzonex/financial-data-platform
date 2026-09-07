#!/bin/sh

set -e

echo "Waiting for MinIO..."

until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
    sleep 2
done

echo "MinIO is ready."

mc mb --ignore-existing local/raw

echo "Bucket 'raw' is ready."
