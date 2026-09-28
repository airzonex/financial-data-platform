#!/bin/sh

set -e

echo "Waiting for MinIO..."

attempts=0
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
	attempts=$((attempts + 1))
	if [ "$attempts" -ge 30 ]; then
		echo "MinIO did not become ready in time" >&2
		exit 1
	fi
    sleep 2
done

echo "MinIO is ready."

mc mb --ignore-existing local/raw

echo "Bucket 'raw' is ready."
