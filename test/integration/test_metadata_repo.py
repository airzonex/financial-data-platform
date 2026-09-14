from datetime import date

import psycopg
import pytest

from config import TEST_DB_CONN_STR

from financial_data.metadata_repo import PipelineInfo
from financial_data.ingestion import IngestionMetrics
from financial_data.staging import StagingMetrics


PIPELINE_NAME = 'financial_test'


def test_create_pipeline_run(metadata_repo) -> None:
    """
    Pipeline run успешно создается
    """
    result = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    assert isinstance(result, PipelineInfo)
    assert result.pipeline_run_id > 0
    assert result.pipeline_run_date is not None

def test_create_pipeline_run_persists_data(metadata_repo) -> None:
    """
    Pipeline run успешно создается с корректными pipeline_name и status
    """
    pipeline_run = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pipeline_name, status
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
                (pipeline_run.pipeline_run_id,)
            )
            result = cur.fetchone()

    assert result == (PIPELINE_NAME, 'running')

def test_create_dataset_run(metadata_repo) -> None:
    """
    Dataset run успешно создается с корректными ipeline_run_id, dataset, requested_start и status 
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    requested_start = date(2014, 1, 1)

    run_id = metadata_repo.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset='rgbi',
        requested_start=requested_start
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pipeline_run_id, dataset, requested_start, status
                  FROM metadata.dataset_runs
                 WHERE id = %s
                """,
                (run_id,)
            )
            result = cur.fetchone()

    assert result == (
        pipeline.pipeline_run_id,
        'rgbi',
        requested_start,
        'running'
    )

def test_determine_date_start_returns_default_for_new_dataset(metadata_repo) -> None:
    """
    determine_date_start возвращает дефолтную дату, если в dataset_runs нет датасета
    """
    result = metadata_repo.determine_date_start('rgbi')

    assert result == date(2014, 1, 1)

def test_determine_date_start_returns_day_after_last_success(metadata_repo) -> None:
    """
    determine_date_start возвращает actual_max_date + 1 day
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    run_id = metadata_repo.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset='rgbi',
        requested_start=date(2014, 1, 1)
    )

    metadata_repo.finish_dataset_run(
        run_id=run_id,
        actual_max_date=date(2026, 9, 10)
    )

    result = metadata_repo.determine_date_start('rgbi')

    assert result == date(2026, 9, 11)

def test_determine_date_start_ignores_failed_runs(metadata_repo) -> None:
    """
    determine_date_start игнорирует dataset runs в статусе error
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    run_id = metadata_repo.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset='rgbi',
        requested_start=date(2014, 1, 1)
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.dataset_runs
                   SET status = 'error',
                       actual_max_date = now()
                 WHERE id = %s
                """,
                (run_id,)
            )

    result = metadata_repo.determine_date_start('rgbi')

    assert result == date(2014, 1, 1)

def test_finish_pipeline_step_success(metadata_repo):
    """
    finish_pipeline_step happy path
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    step_id = metadata_repo.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='ingestion_rgbi',
        step_type='ingestion'
    )

    metadata_repo.finish_pipeline_step(
        step_id=step_id,
        status='success',
        records_in=100,
        records_out=95
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, records_in, records_out, error, finished_at
                  FROM metadata.pipeline_steps
                 WHERE id = %s
                """,
                (step_id,)
            )
            result = cur.fetchone()

    assert result[0] == 'success'
    assert result[1] == 100
    assert result[2] == 95
    assert result[3] is None
    assert result[4] is not None

@pytest.mark.parametrize(
    ('status'),
    [
        ('success'),
        ('error')
    ]
)
def test_finish_pipeline_step_cannot_finish_not_running_step(metadata_repo, status) -> None:
    """
    finish_pipeline_step raises ValueError когда пытаешься завершить step не в статусе running
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    step_id = metadata_repo.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='ingestion_rgbi',
        step_type='ingestion'
    )

    metadata_repo.finish_pipeline_step(
        step_id=step_id,
        status=status
    )

    with pytest.raises(ValueError):
        metadata_repo.finish_pipeline_step(
            step_id=step_id,
            status=status
        )

def test_finish_pipeline_run_is_success_when_all_steps_succeed(metadata_repo) -> None:
    """
    finish_pipeline_run завершается со статусом success, если все steps завершились success 
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    step1 = metadata_repo.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='ingestion_rgbi',
        step_type='ingestion'
    )

    step2 = metadata_repo.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='staging_rgbi',
        step_type='staging'
    )

    metadata_repo.finish_pipeline_step(step1, 'success')
    metadata_repo.finish_pipeline_step(step2, 'success')

    metadata_repo.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, error, finished_at
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
                (pipeline.pipeline_run_id,)
            )
            res = cur.fetchone()

    assert res[0] == 'success'
    assert res[1] == ''
    assert res[2] is not None

def test_finish_pipeline_run_error_if_step_fails(metadata_repo) -> None:
    """
    finish_pipeline_run завершается с error если хотя бы один step завершился c error
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    success_step = metadata_repo.start_pipeline_step(
        pipeline.pipeline_run_id,
        'ingestion_rgbi',
        'ingestion'
    )

    error_step = metadata_repo.start_pipeline_step(
        pipeline.pipeline_run_id,
        'staging_rgbi',
        'staging'
    )

    metadata_repo.finish_pipeline_step(
        step_id=success_step,
        status='success'
    )

    metadata_repo.finish_pipeline_step(
        step_id=error_step,
        status='error',
        error='staging error'
    )

    metadata_repo.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, error, finished_at
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
                (pipeline.pipeline_run_id,)
            )
            res = cur.fetchone()

    assert res[0] == 'error'
    assert res[1] == 'staging error'
    assert res[2] is not None

def test_finish_pipeline_run_is_running_when_step_is_running(metadata_repo) -> None:
    """
    finish_pipeline_run завершает run со статусом running, если какой то из steps в running
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    step_id = metadata_repo.start_pipeline_step(
        pipeline.pipeline_run_id,
        'ingestion_rgbi',
        'ingestion'
    )

    metadata_repo.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, finished_at
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
                (pipeline.pipeline_run_id,)
            )
            res = cur.fetchone()

    assert res[0] == 'running'
    assert res[1] is not None

def test_finish_pipeline_run_raises_when_no_steps(metadata_repo) -> None:
    """
    finish_pipeline_run райзит ValueError если не нашел pipeline steps 
    """
    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    with pytest.raises(
        ValueError,
        match=f'Pipeline run {pipeline.pipeline_run_id} was not finished, no pipeline steps found'
    ):
        metadata_repo.finish_pipeline_run(pipeline.pipeline_run_id)

def test_add_ingestion_metrics(metadata_repo) -> None:
    """
    add_ingestion_metrics happy path
    """
    bucket = 'raw'
    prefix = '/moex/rgbi/ingestion_date=2026-09-10/run_id=1'
    objects_cnt = 15

    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    run_id = metadata_repo.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset='rgbi',
        requested_start=date(2014, 1, 1)
    )

    metrics = IngestionMetrics(
        run_id=run_id,
        bucket=bucket,
        objects_prefix=prefix,
        objects_saved=objects_cnt
    )

    metadata_repo.add_ingestion_metrics(metrics)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT minio_bucket, minio_prefix, objects_saved
                  FROM metadata.dataset_runs
                 WHERE id = %s
                """,
                (run_id,)
            )
            res = cur.fetchone()

    assert res[0] == bucket
    assert res[1] == prefix
    assert res[2] == objects_cnt

def test_add_ingestion_metrics_raises_when_no_dataset_run(metadata_repo) -> None:
    """
    add_ingestion_metrics райзит ValueError если не нашел dataset run
    """
    run_id = 999999

    metrics = IngestionMetrics(
        run_id=run_id,
        bucket='raw',
        objects_prefix='/moex/some/prefix',
        objects_saved=1
    )

    with pytest.raises(
        ValueError,
        match=f'Metrics for {run_id} dataset run were not added'
    ):
        metadata_repo.add_ingestion_metrics(metrics)

def test_add_staging_metrics(metadata_repo) -> None:
    """
    add_staging_metrics happy path
    """
    loaded = 100

    pipeline = metadata_repo.create_pipeline_run(PIPELINE_NAME)

    run_id = metadata_repo.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset='rgbi',
        requested_start=date(2026, 1, 1)
    )

    metrics = StagingMetrics(
        run_id=run_id,
        records_loaded=loaded
    )

    metadata_repo.add_staging_metrics(metrics)

    with psycopg.connect(TEST_DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT records_loaded
                  FROM metadata.dataset_runs
                 WHERE id = %s
                """,
                (run_id,)
            )
            res = cur.fetchone()

    assert res[0] == loaded

def test_add_staging_metrics_raises_when_no_dataset_run(metadata_repo) -> None:
    """
    add_staging_metrics райзит ValueError если не находит dataset run
    """
    run_id = 999999

    metrics = StagingMetrics(
        run_id=run_id,
        records_loaded=150
    )

    with pytest.raises(
        ValueError,
        match=f'Metrics for {run_id} dataset run were not added'
    ):
        metadata_repo.add_staging_metrics(metrics)