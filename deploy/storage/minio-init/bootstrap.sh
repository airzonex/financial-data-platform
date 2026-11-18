#!/bin/sh
set -eu

: "${MINIO_ROOT_USER:?Missing admin user}"
: "${MINIO_ROOT_PASSWORD:?Missing admin password}"
: "${MINIO_APP_USER:?Missing application user}"
: "${MINIO_APP_PASSWORD:?Missing application password}"

# Keep temporary client credentials inside this disposable container.
export MC_CONFIG_DIR=/tmp/mc-config

mc alias set storage http://minio:9000 \
    "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

attempts=0
until mc ls storage >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
        echo "MinIO did not become ready." >&2
        exit 1
    fi
    sleep 2
done

mc mb --ignore-existing storage/raw

mc admin policy create \
    storage fdp-raw-access /init/raw-policy.json

mc admin user add \
    storage "$MINIO_APP_USER" "$MINIO_APP_PASSWORD"

mc admin policy attach \
    storage fdp-raw-access --user "$MINIO_APP_USER"

# Verify access using the application credentials.
mc alias set app http://minio:9000 \
    "$MINIO_APP_USER" "$MINIO_APP_PASSWORD" >/dev/null

printf 'fdp-storage-smoke-test\n' > /tmp/expected.txt
mc cp /tmp/expected.txt app/raw/_checks/bootstrap.txt
mc cat app/raw/_checks/bootstrap.txt > /tmp/actual.txt
cmp /tmp/expected.txt /tmp/actual.txt

# Cleanup uses the administrator because the application cannot delete.
mc rm storage/raw/_checks/bootstrap.txt

echo "MINIO_BOOTSTRAP_PASSED"