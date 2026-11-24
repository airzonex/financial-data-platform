BEGIN;

ALTER TABLE metadata.pipeline_runs
    ADD COLUMN IF NOT EXISTS request_key text;

CREATE UNIQUE INDEX IF NOT EXISTS pipeline_runs_request_key_uq
    ON metadata.pipeline_runs (pipeline_name, request_key);

COMMIT;