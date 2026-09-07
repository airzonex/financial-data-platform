create table if not exists metadata.pipeline_runs(
    id              bigint generated always as identity primary key,
    pipeline_name   text not null,
    started_at      timestamptz not null default now(),
    finished_at     timestamptz,
    status          text not null
                    check (status in ('running', 'success', 'error')),
    error           text
);


create table if not exists metadata.pipeline_steps(
    id              bigint generated always as identity primary key,
    pipeline_run_id bigint not null
                    references metadata.pipeline_runs(id),

    step_name       text not null,
    step_type       text not null
                    check (step_type in (
                        'ingestion',
                        'staging',
                        'transformation'
                    )),

    started_at      timestamptz not null default now(),
    finished_at     timestamptz,

    status          text not null
                    check (status in ('running', 'success', 'error')),

    records_in      bigint,
    records_out     bigint,

    error           text,

    unique (pipeline_run_id, step_name)
);


create table if not exists metadata.dataset_runs(
    id              bigint generated always as identity primary key,

    pipeline_run_id bigint not null
                    references metadata.pipeline_runs(id),

    dataset         text not null,

    requested_start date not null,

    actual_max_date date,
	records_loaded	int,

    status          text not null
                    check (status in ('running', 'success', 'error')),

    minio_bucket    text,
    minio_prefix    text,
    objects_saved   bigint
);

#create table if not exists metadata.ingestion_runs(
#	id				bigint generated always as identity primary key,
#	source			text not null,
#	dataset			text not null,
#	requested_start date not null,
#	actual_min_date date,
#	actual_max_date date,
#	started_at		timestamptz not null default now(),
#	finished_at		timestamptz,
#	status			text not null check (status in ('running', 'success', 'error')),
#	records_received integer,
#	minio_bucket	text,
#	minio_prefix 	text,
#	objects_saved	int,
#	error 			text
#);

#CREATE INDEX IF NOT EXISTS idx_ingestion_runs_dataset_status
#    ON metadata.ingestion_runs (dataset, status);
#
#CREATE INDEX IF NOT EXISTS idx_ingestion_runs_dataset_actual_max_date
#    ON metadata.ingestion_runs (dataset, actual_max_date);
