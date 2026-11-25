from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal
from uuid import UUID, uuid4

import psycopg

from financial_data.config import (
    KEYRATE_DATASET,
    PIPELINE_NAME,
    RGBI_DATASET,
)
from financial_data.ingestion import IngestionMetrics
from financial_data.staging import StagingMetrics

DEFAULT_DATE_START = date(2014, 1, 1)


@dataclass(frozen=True)
class PipelineInfo:
    pipeline_run_id: int
    pipeline_run_date: date

@dataclass(frozen=True)
class StepsStatus:
    status: str
    error: str

@dataclass
class DatasetRunInfo:
    run_id: int
    requested_start: date
    dataset: str

@dataclass
class StepClaim:
    step_id: int
    attempt_id: UUID | None
    already_completed: bool


class MetadataRepository:

    def __init__(self, connection_str: str) -> None:
        self.connection_str = connection_str

    def create_pipeline_run(self, pipeline_name: str) -> PipelineInfo:
        query = """
            INSERT INTO metadata.pipeline_runs(pipeline_name, status)
            VALUES (%s, %s)
            RETURNING id, started_at::date
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (pipeline_name, 'running'))
            row = cur.fetchone()

            if row is None:
                raise ValueError(f'Failed to create pipeline run for {pipeline_name}')

            run_id, run_date = row[0:2]

        return PipelineInfo(run_id, run_date)

    def __get_last_successful_date(self, dataset: str) -> date | None:
        query = """
            SELECT MAX(actual_max_date)
              FROM metadata.dataset_runs
             WHERE dataset = %s
               AND status = %s
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (dataset, 'success'))
            row = cur.fetchone()

        if row is None:
            return None

        return row[0]

    def determine_date_start(self, dataset: str) -> date:
        actual_max_date = self.__get_last_successful_date(dataset)

        if actual_max_date is None:
            return DEFAULT_DATE_START

        return actual_max_date + timedelta(days=1)

    def create_dataset_run(self, pipeline_run_id: int, dataset: str, requested_start: date) -> int:
        query = """
            INSERT INTO metadata.dataset_runs(pipeline_run_id, dataset, requested_start, status)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (pipeline_run_id, dataset, requested_start, 'running'))
            row = cur.fetchone()

            if row is None:
                raise ValueError(f'Failed to create dataset run for {dataset}')

        return row[0]

    def start_pipeline_step(
            self, 
            pipeline_run_id: int, 
            step_name: str, 
            step_type: Literal['ingestion', 'staging', 'transformation']
        ) -> int:
        query = """
            INSERT INTO metadata.pipeline_steps(pipeline_run_id, step_name, step_type, status)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (pipeline_run_id, step_name, step_type, 'running'))
            row = cur.fetchone()

            if row is None:
                raise ValueError(f'Failed to start pipeline step {step_name}')

        return row[0]

    def finish_pipeline_step(
            self, 
            step_id: int, 
            status: Literal['success', 'error'], 
            records_in: int | None = None, 
            records_out: int | None = None, 
            error: str | None = None) -> None:
        query = """
            UPDATE metadata.pipeline_steps
               SET status = %s,
                   records_in = %s,
                   records_out = %s,
                   error = %s,
                   finished_at = now()
             WHERE id = %s
               AND status = 'running'
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (status, records_in, records_out, error, step_id))

            if cur.rowcount != 1:
                raise ValueError(
                    f'Step {step_id} was not updated (params: status - {status}, '
                    f'records_in - {records_in}, records_out - {records_out}, error - {error})'
                )

    def add_ingestion_metrics(self, ingestion_result: IngestionMetrics) -> None:
        query = """
            UPDATE metadata.dataset_runs
               SET minio_bucket = %s,
                   minio_prefix = %s,
                   objects_saved = %s
             WHERE id = %s
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (
                ingestion_result.bucket,
                ingestion_result.objects_prefix,
                ingestion_result.objects_saved,
                ingestion_result.run_id
                ))

            if cur.rowcount != 1:
                raise ValueError(
                    f'Metrics for {ingestion_result.run_id} dataset run were not added'
                )

    def add_staging_metrics(self, staging_result: StagingMetrics) -> None:
        query = """
            UPDATE metadata.dataset_runs
               SET records_loaded = %s
             WHERE id = %s
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (staging_result.records_loaded, staging_result.run_id))

            if cur.rowcount != 1:
                raise ValueError(
                    f'Metrics for {staging_result.run_id} dataset run were not added'
                )


    def finish_dataset_run(self, run_id: int, actual_max_date: date) -> None:
        query = """
            UPDATE metadata.dataset_runs
                SET status = 'success',
                    actual_max_date = %s
                WHERE id = %s
                  AND status = 'running'
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:
            cur.execute(query, (actual_max_date, run_id))

            if cur.rowcount != 1:
                raise ValueError(
                    f'Dataset run {run_id} was not finished, no data found for update'
                )

    def __pipeline_steps_status(self, pipeline_run_id: int, cur: psycopg.Cursor) -> StepsStatus | None:
        query = """
            SELECT status, error
              FROM metadata.pipeline_steps
             WHERE pipeline_run_id = %s
        """

        cur.execute(query, (pipeline_run_id, ))

        rows = cur.fetchall()

        if not rows:
            return None

        if any(status == 'error' for status, _ in rows):
            errors = [error for status, error in rows if status == 'error']
            return StepsStatus('error', '; '.join(errors))

        if any(status == 'running' for status, _ in rows):
            return StepsStatus('running', '')

        return StepsStatus('success', '')


    def finish_pipeline_run(self, pipeline_run_id: int) -> None:

        query = """
            UPDATE metadata.pipeline_runs
               SET status = %s,
                   error = %s,
                   finished_at = now()
             WHERE id = %s
        """

        with psycopg.connect(self.connection_str) as conn, conn.cursor() as cur:

            steps_status = self.__pipeline_steps_status(pipeline_run_id, cur)

            if steps_status is None:
                raise ValueError(
                    f'Pipeline run {pipeline_run_id} was not finished, no pipeline steps found'
                )

            cur.execute(query, (steps_status.status, steps_status.error, pipeline_run_id))

            if cur.rowcount != 1:
                raise ValueError(
                    f'Pipeline run {pipeline_run_id} was not finished, no rows to update'
                )


    def prepare_dataset_run(
        self,
        pipeline_run_id: int,
        dataset: str,
    ) -> DatasetRunInfo:
        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            # serialize preparation for this pipeline run
            cur.execute(
                """
                SELECT status
                FROM metadata.pipeline_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (pipeline_run_id,),
            )
            pipeline = cur.fetchone()

            if pipeline is None:
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} does not exist"
                )

            if pipeline[0] != "running":
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} is not running"
                )

            cur.execute(
                """
                SELECT id, requested_start
                FROM metadata.dataset_runs
                WHERE pipeline_run_id = %s
                  AND dataset = %s
                """,
                (pipeline_run_id, dataset),
            )
            existing = cur.fetchall()

            if len(existing) > 1:
                raise ValueError(
                    f"Multiple runs found for pipeline "
                    f"{pipeline_run_id}, dataset {dataset}"
                )

            if existing:
                run_id, requested_start = existing[0]
            else:
                requested_start = self.determine_date_start(dataset)

                cur.execute(
                    """
                    INSERT INTO metadata.dataset_runs (
                        pipeline_run_id,
                        dataset,
                        requested_start,
                        status
                    )
                    VALUES (%s, %s, %s, 'running')
                    RETURNING id
                    """,
                    (pipeline_run_id, dataset, requested_start),
                )
                row = cur.fetchone()

                if row is None:
                    raise RuntimeError("Failed to create dataset run")

                run_id = row[0]

        return DatasetRunInfo(
            run_id=run_id,
            requested_start=requested_start,
            dataset=dataset,
        )


    def get_ingestion_context(
        self,
        dataset_run_id: int,
    ) -> tuple[PipelineInfo, DatasetRunInfo]:
        query = """
            SELECT p.id,
                   p.started_at::date,
                   p.status,
                   d.dataset,
                   d.requested_start,
                   d.status
            FROM metadata.dataset_runs d
            JOIN metadata.pipeline_runs p
              ON p.id = d.pipeline_run_id
            WHERE d.id = %s
        """

        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(query, (dataset_run_id,))
            row = cur.fetchone()

        if row is None:
            raise ValueError(
                f"Dataset run {dataset_run_id} does not exist"
            )

        (
            pipeline_id,
            pipeline_date,
            pipeline_status,
            dataset,
            requested_start,
            dataset_status,
        ) = row

        if pipeline_status != "running" or dataset_status != "running":
            raise ValueError(
                f"Dataset run {dataset_run_id} and its pipeline must both be running"
            )

        return (
            PipelineInfo(pipeline_id, pipeline_date),
            DatasetRunInfo(dataset_run_id, requested_start, dataset),
        )


    def get_staging_input(
        self,
        dataset_run_id: int,
    ) -> IngestionMetrics:
        query = """
        SELECT r.minio_bucket,
               r.minio_prefix,
               r.objects_saved,
               s.status
          FROM metadata.dataset_runs r
          LEFT JOIN metadata.pipeline_steps s
            ON s.pipeline_run_id = r.pipeline_run_id
           AND s.step_type = 'ingestion'
           AND s.step_name = 'ingestion_' || r.dataset
         WHERE r.id = %s
        """

        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(query, (dataset_run_id,))
            row = cur.fetchone()

        if row is None:
            raise ValueError(f"Dataset run {dataset_run_id} does not exist")

        bucket, prefix, objects_saved, ingestion_status = row

        if ingestion_status != "success":
            raise ValueError(
                f"Ingestion has not succeeded for dataset run {dataset_run_id}"
            )

        if not bucket or not prefix or objects_saved is None:
            raise ValueError(
                f"Ingestion metadata is incomplete for dataset run {dataset_run_id}"
            )

        if objects_saved <= 0:
            raise ValueError(
                f"No raw objects available for dataset run {dataset_run_id}"
            )

        return IngestionMetrics(
            run_id=dataset_run_id,
            bucket=bucket,
            objects_prefix=prefix,
            objects_saved=objects_saved,
        )


    def require_ready_for_dbt(
        self,
        pipeline_run_id: int,
    ) -> None:
        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT pipeline_name,
                       status
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
                (pipeline_run_id,)
            )
            pipeline = cur.fetchone()

            if pipeline is None:
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} does not exist"
                )

            if pipeline != (PIPELINE_NAME, "running"):
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} must belong to "
                    f"{PIPELINE_NAME} and be running"
                )

            cur.execute(
                """
                SELECT d.dataset,
                       d.status,
                       d.records_loaded,
                       s.status
                FROM metadata.dataset_runs d
                LEFT JOIN metadata.pipeline_steps s
                  ON d.pipeline_run_id = s.pipeline_run_id
                 AND s.step_name = 'staging_' || d.dataset
                 AND s.step_type = 'staging'
                WHERE d.pipeline_run_id = %s
                """,
                (pipeline_run_id,)
            )
            rows = cur.fetchall()

        expected = {KEYRATE_DATASET, RGBI_DATASET}

        if len(rows) != 2 or {row[0] for row in rows} != expected:
            raise ValueError(
                "Expected exactly one RGBI and one KEYRATE dataset run"
            )

        for dataset, dataset_status, records_loaded, staging_status in rows:
            if dataset_status != "running":
                raise ValueError(
                    f"Dataset {dataset} is not running"
                )

            if staging_status != "success":
                raise ValueError(
                    f"Dataset {dataset} staging status is {staging_status}, it must be success"
                )

            if records_loaded is None or records_loaded <= 0:
                raise ValueError(
                    f"No staging rows recorded for dataset {dataset}"
                )


    def finalize_successful_pipeline(
        self,
        pipeline_run_id: int,
    ) -> dict[str, date]:
        watermark_queries = {
            RGBI_DATASET: """
                SELECT MAX(trade_date)
                FROM int.rgbi_history
                WHERE source_run_id = %s
            """,
            KEYRATE_DATASET: """
                SELECT MAX(trade_date)
                FROM int.keyrate_history
                WHERE source_run_id = %s
            """
        }

        expected_steps = {
            "ingestion_rgbi",
            "ingestion_keyrate",
            "staging_rgbi",
            "staging_keyrate",
            "dbt_build",
        }

        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT pipeline_name, status
                FROM metadata.pipeline_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (pipeline_run_id,)
            )
            pipeline = cur.fetchone()

            if pipeline is None:
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} does not exist"
                )

            pipeline_name, pipeline_status = pipeline

            if pipeline_name != PIPELINE_NAME:
                raise ValueError("Unexpected pipeline name")

            if pipeline_status not in ("running", "success"):
                raise ValueError(
                    f"Cannot finalize pipeline with status {pipeline_status}"
                )

            cur.execute(
                """
                SELECT step_name, status
                FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                """,
                (pipeline_run_id,),
            )
            steps = cur.fetchall()

            if (
                len(steps) != len(expected_steps)
                or {name for name, _ in steps} != expected_steps
                or any(status != "success" for _, status in steps)
            ):
                raise ValueError(
                    "All five expected pipeline steps must be successful"
                )

            cur.execute(
                """
                SELECT id, dataset, status, actual_max_date
                FROM metadata.dataset_runs
                WHERE pipeline_run_id = %s
                ORDER BY id
                FOR UPDATE
                """,
                (pipeline_run_id,)
            )
            datasets = cur.fetchall()

            if (
                len(datasets) != 2
                or {row[1] for row in datasets}
                != {RGBI_DATASET, KEYRATE_DATASET}
            ):
                raise ValueError(
                    "Expected exactly one run for each dataset"
                )

            watermarks: dict[str, date] = {}

            # a retry after a successful commit returns persistent results
            if pipeline_status == "success":
                for _, dataset, status, actual_max_date in datasets:
                    if status != "success" or actual_max_date is None:
                        raise ValueError(
                            "Completed pipeline has inconsistent dataset metadata"
                        )
                    watermarks[dataset] = actual_max_date

                return watermarks

            for run_id, dataset, status, _ in datasets:
                if status != "running":
                    raise ValueError(
                        f"Dataset run {run_id} is not running"
                    )

                cur.execute(watermark_queries[dataset], (run_id,))
                row = cur.fetchone()

                if row is None or row[0] is None:
                    raise ValueError(
                        f"No transformed data found for dataset run {run_id}"
                    )

                watermarks[dataset] = row[0]

            # both watermarks have been validated before updating anything
            for run_id, dataset, _, _ in datasets:
                cur.execute(
                    """
                    UPDATE metadata.dataset_runs
                       SET status = 'success',
                           actual_max_date = %s
                     WHERE id = %s
                    """,
                    (watermarks[dataset], run_id),
                )

            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                   SET status = 'success',
                       finished_at = now(),
                       error = NULL
                 WHERE id = %s
                """,
                (pipeline_run_id,),
            )

        return watermarks


    def get_or_create_pipeline_run(
        self,
        pipeline_name: str,
        request_key: str,
    ) -> PipelineInfo:
        if not request_key.strip():
            raise ValueError("Request key must not be empty")

        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs (
                    pipeline_name,
                    request_key,
                    status
                )
                VALUES (%s, %s, 'running')
                ON CONFLICT (pipeline_name, request_key) DO NOTHING
                RETURNING id, started_at::date
                """,
                (pipeline_name, request_key),
            )
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    """
                    SELECT id, started_at::date
                    FROM metadata.pipeline_runs
                    WHERE pipeline_name = %s
                      AND request_key = %s
                    """,
                    (pipeline_name, request_key),
                )
                row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    "Could not create or retrieve pipeline run"
                )

            pipeline = PipelineInfo(
                pipeline_run_id=row[0],
                pipeline_run_date=row[1],
            )

        return pipeline


    def claim_pipeline_step(
        self,
        pipeline_run_id: int,
        step_name: str,
        step_type: Literal["ingestion", "staging", "transformation"],
    ) -> StepClaim:
        with (
            psycopg.connect(self.connection_str) as conn,
            conn.cursor() as cur,
        ):
            # serialize claim decisions within this pipeline
            cur.execute(
                """
                SELECT status
                FROM metadata.pipeline_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (pipeline_run_id,)
            )
            pipeline = cur.fetchone()

            if pipeline is None:
                raise ValueError(
                    f"Pipeline run {pipeline_run_id} does not exist"
                )

            pipeline_status = pipeline[0]

            if pipeline_status not in ("running", "success"):
                raise ValueError(
                    f"Cannot claim a step in pipeline status {pipeline_status}"
                )

            cur.execute(
                """
                SELECT id, step_type, status
                FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                  AND step_name = %s
                FOR UPDATE
                """,
                (pipeline_run_id, step_name),
            )
            step = cur.fetchone()

            if step is not None:
                step_id, stored_type, status = step

                if stored_type != step_type:
                    raise ValueError(
                        f"Step type mismatch for {step_name}"
                    )

                if status == "success":
                    return StepClaim(
                        step_id=step_id,
                        attempt_id=None,
                        already_completed=True,
                    )

            if pipeline_status == "success":
                raise ValueError(
                    "Cannot execute a step in a completed pipeline"
                )

            attempt_id = uuid4()

            if step is None:
                cur.execute(
                    """
                    INSERT INTO metadata.pipeline_steps (
                        pipeline_run_id,
                        step_name,
                        step_type,
                        status,
                        attempt_id,
                        attempt_count
                    )
                    VALUES (%s, %s, %s, 'running', %s, 1)
                    RETURNING id
                    """,
                    (
                        pipeline_run_id,
                        step_name,
                        step_type,
                        attempt_id
                    ),
                )
                inserted = cur.fetchone()

                if inserted is None:
                    raise RuntimeError("Failed to create step")

                step_id = inserted[0]
            else:
                step_id, _, status = step

                if status == "running":
                    raise ValueError(
                        f"Step {step_name} is already running"
                    )

                if status != "error":
                    raise ValueError(
                        f"Cannot retry step with status {status}"
                    )

                cur.execute(
                    """
                    UPDATE metadata.pipeline_steps
                    SET status = 'running',
                        attempt_id = %s,
                        attempt_count = attempt_count + 1,
                        started_at = now(),
                        finished_at = NULL,
                        error = NULL,
                        records_in = NULL,
                        records_out = NULL
                    WHERE id = %s
                    """,
                    (attempt_id, step_id),
                )

        return StepClaim(
            step_id=step_id,
            attempt_id=attempt_id,
            already_completed=False,
        )