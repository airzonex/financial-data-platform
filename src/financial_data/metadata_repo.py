from datetime import date, timedelta
from dataclasses import dataclass
from typing import Literal

from financial_data.ingestion import IngestionMetrics
from financial_data.staging import StagingMetrics

import psycopg


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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pipeline_name, 'running'))
                run_id, run_date = cur.fetchone()[0:2]

        return PipelineInfo(run_id, run_date)

    def __get_last_successful_date(self, dataset: str) -> date | None:
        query = """
            SELECT MAX(actual_max_date)
              FROM metadata.dataset_runs
             WHERE dataset = %s
               AND status = %s
        """

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (dataset, 'success'))
                res = cur.fetchone()[0]

        return res

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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pipeline_run_id, dataset, requested_start, 'running'))
                res = cur.fetchone()[0]

        return res

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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pipeline_run_id, step_name, step_type, 'running'))
                res = cur.fetchone()[0]

        return res

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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
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

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:

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

