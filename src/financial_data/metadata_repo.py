from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import psycopg

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