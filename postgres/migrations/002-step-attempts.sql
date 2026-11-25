BEGIN;

ALTER TABLE metadata.pipeline_steps
    ADD COLUMN IF NOT EXISTS attempt_id uuid,
    ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0;

COMMIT;