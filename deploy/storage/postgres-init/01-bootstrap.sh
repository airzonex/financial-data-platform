#!/usr/bin/env bash
set -euo pipefail

psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --set app_password="$FDP_APP_PASSWORD" <<'SQL'
BEGIN;

CREATE ROLE fdp_app
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    PASSWORD :'app_password';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

GRANT CONNECT, CREATE ON DATABASE financial TO fdp_app;
GRANT USAGE, CREATE ON SCHEMA public TO fdp_app;

SET ROLE fdp_app;

\i /opt/fdp/schema/01-init.sql
\i /opt/fdp/schema/02-metadata.sql
\i /opt/fdp/schema/03-stg.sql

COMMIT;
SQL