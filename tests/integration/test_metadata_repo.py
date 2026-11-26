from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Literal
from unittest.mock import Mock, patch
from uuid import uuid4

import psycopg
import pytest
from config import TEST_DB_CONN_STR

from financial_data.config import PIPELINE_NAME as APP_PIPELINE_NAME
from financial_data.ingestion import IngestionMetrics
from financial_data.jobs import ingest_dataset
from financial_data.metadata_repo import MetadataRepository, PipelineInfo
from financial_data.runtime import RuntimeSettings
from financial_data.staging import KeyrateStagingLoader, StagingMetrics
from financial_data.storage import MinioStorage

PIPELINE_NAME = 'financial_test'


@pytest.fixture(autouse=True)
def clear_metadata_tables(ensure_test_database) -> Iterator[None]:
    """
    фикстура чистит таблицы из metadata перед и после каждого теста
    """

    def _truncate_tables():
        with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
            cur.execute(
                """
                    TRUNCATE
                        metadata.pipeline_steps,
                        metadata.dataset_runs,
                        metadata.pipeline_runs
                    RESTART IDENTITY
                    """
            )

    _truncate_tables()
    yield
    _truncate_tables()


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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    metadata_repo.start_pipeline_step(
        pipeline.pipeline_run_id,
        'ingestion_rgbi',
        'ingestion'
    )

    metadata_repo.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
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


@pytest.mark.parametrize("concurrent", [False, True])
def test_request_key_creates_one_pipeline_run(concurrent: bool) -> None:
    request_key = f"test-{uuid4()}"
    repository = MetadataRepository(TEST_DB_CONN_STR)

    def start():
        return repository.get_or_create_pipeline_run(
            pipeline_name=PIPELINE_NAME,
            request_key=request_key,
        )

    try:
        if concurrent:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(start) for _ in range(2)]
                results = [future.result() for future in futures]
        else:
            results = [start(), start()]

        assert results[0] == results[1]

        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT COUNT(*)
                FROM metadata.pipeline_runs
                WHERE pipeline_name = %s
                  AND request_key = %s
                """,
                (PIPELINE_NAME, request_key),
            )
            assert cur.fetchone() == (1,)
    finally:
        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                DELETE FROM metadata.pipeline_runs
                WHERE pipeline_name = %s
                  AND request_key = %s
                """,
                (PIPELINE_NAME, request_key),
            )


def test_step_claim_retry_and_completed_step() -> None:
    repository = MetadataRepository(TEST_DB_CONN_STR)
    pipeline = repository.get_or_create_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        request_key=f"test-claim-{uuid4()}",
    )
    pipeline_id = pipeline.pipeline_run_id

    try:
        first = repository.claim_pipeline_step(
            pipeline_id,
            "ingestion_rgbi",
            "ingestion",
        )

        assert not first.already_completed
        assert first.attempt_id is not None

        # a running step cannot be claimed again
        with pytest.raises(ValueError, match="already running"):
            repository.claim_pipeline_step(
                pipeline_id,
                "ingestion_rgbi",
                "ingestion",
            )

        # simulate the first attempt reporting failure
        repository.finish_claimed_step(
            step_id=first.step_id,
            attempt_id=first.attempt_id,
            status="error",
            error="Temporary source failure",
        )

        second = repository.claim_pipeline_step(
            pipeline_id,
            "ingestion_rgbi",
            "ingestion",
        )

        assert second.step_id == first.step_id
        assert second.attempt_id is not None
        assert second.attempt_id != first.attempt_id
        assert not second.already_completed

        # the old attempt cannot complete or fail the new attempt
        stale_statuses: tuple[
            Literal["success", "error"], ...
        ] = ("success", "error")

        for stale_status in stale_statuses:
            with pytest.raises(ValueError, match="not running under"):
                repository.finish_claimed_step(
                    step_id=first.step_id,
                    attempt_id=first.attempt_id,
                    status=stale_status,
                )

        # rejected updates must leave the new attempt unchanged
        with(
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT status, attempt_id, attempt_count, finished_at
                FROM metadata.pipeline_steps
                WHERE id = %s
                """,
                (second.step_id,),
            )
            assert cur.fetchone() == (
                'running',
                second.attempt_id,
                2,
                None,
            )

        repository.finish_claimed_step(
            step_id=second.step_id,
            attempt_id=second.attempt_id,
            status="success",
        )

        completed = repository.claim_pipeline_step(
            pipeline_id,
            "ingestion_rgbi",
            "ingestion",
        )

        assert completed.step_id == first.step_id
        assert completed.already_completed
        assert completed.attempt_id is None

        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT status, attempt_count
                FROM metadata.pipeline_steps
                WHERE id = %s
                """,
                (first.step_id,),
            )
            assert cur.fetchone() == ("success", 2)

    finally:
        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                DELETE FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            cur.execute(
                """
                DELETE FROM metadata.pipeline_runs WHERE id = %s
                """,
                (pipeline_id,),
            )


def test_ingestion_completion_is_atomic_and_rejects_stale_attempt() -> None:
    repository = MetadataRepository(TEST_DB_CONN_STR)
    pipeline = repository.get_or_create_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        request_key=f"test-ingestion-completion-{uuid4()}",
    )
    pipeline_id = pipeline.pipeline_run_id

    try:
        dataset_id = repository.create_dataset_run(
            pipeline_run_id=pipeline_id,
            dataset="rgbi",
            requested_start=date(2014, 1, 1),
        )

        first = repository.claim_pipeline_step(
            pipeline_id,
            "ingestion_rgbi",
            "ingestion",
        )
        assert first.attempt_id is not None

        repository.finish_claimed_step(
            step_id=first.step_id,
            attempt_id=first.attempt_id,
            status="error",
            error="Temporary failure",
        )

        second = repository.claim_pipeline_step(
            pipeline_id,
            "ingestion_rgbi",
            "ingestion",
        )
        assert second.attempt_id is not None

        valid_result = IngestionMetrics(
            run_id=dataset_id,
            bucket="raw",
            objects_prefix=f"test/attempt_id={second.attempt_id}",
            objects_saved=2,
        )

        # as old attempt cannot publish its result
        with pytest.raises(ValueError, match="not running"):
            repository.complete_ingestion(
                step_id=first.step_id,
                attempt_id=first.attempt_id,
                result=valid_result,
            )

        # a mismatched dataset must roll back the step's success update
        invalid_result = IngestionMetrics(
            run_id=-1,
            bucket="raw",
            objects_prefix="test/invalid",
            objects_saved=2,
        )

        with pytest.raises(ValueError, match="does not match"):
            repository.complete_ingestion(
                step_id=second.step_id,
                attempt_id=second.attempt_id,
                result=invalid_result,
            )

        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT status, finished_at
                FROM metadata.pipeline_steps
                WHERE id = %s
                """,
                (second.step_id,),
            )
            assert cur.fetchone() == ('running', None)

            cur.execute(
                """
                SELECT minio_bucket, minio_prefix, objects_saved
                FROM metadata.dataset_runs
                WHERE id = %s
                """,
                (dataset_id,),
            )
            assert cur.fetchone() == (None, None, None)

        repository.complete_ingestion(
            step_id=second.step_id,
            attempt_id=second.attempt_id,
            result=valid_result,
        )

        with (
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT s.status,
                       d.minio_bucket,
                       d.minio_prefix,
                       d.objects_saved
                FROM metadata.pipeline_steps s
                JOIN metadata.dataset_runs d
                  ON s.pipeline_run_id = d.pipeline_run_id
                WHERE s.id = %s
                  AND d.id = %s
                """,
                (second.step_id, dataset_id),
            )
            assert cur.fetchone() == (
                "success",
                valid_result.bucket,
                valid_result.objects_prefix,
                valid_result.objects_saved
            )

    finally:
        with(
            psycopg.connect(TEST_DB_CONN_STR) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                DELETE FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            cur.execute(
                """
                DELETE FROM metadata.dataset_runs
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            cur.execute(
                """
                DELETE FROM metadata.pipeline_runs
                WHERE id = %s
                """,
                (pipeline_id,),
            )


def test_ingestion_job_retries_failed_attempt() -> None:
    repo = MetadataRepository(TEST_DB_CONN_STR)

    settings = Mock(spec=RuntimeSettings)
    settings.database_conninfo.return_value = TEST_DB_CONN_STR

    pipeline = repo.get_or_create_pipeline_run(
        PIPELINE_NAME,
        f"test-ingestion-retry-{uuid4()}",
    )
    pipeline_id = pipeline.pipeline_run_id

    attempted_prefixes: list[str] = []

    try:
        dataset = repo.prepare_dataset_run(pipeline_id, "keyrate")
        dataset_id = dataset.run_id

        def simulate_ingestion(
            *args,
            attempt_id,
            **kwargs,
        ) -> IngestionMetrics:
            prefix = (
                f"cbr/keyrate/run_id={dataset_id}"
                f"/attempt_id={attempt_id}"
            )
            attempted_prefixes.append(prefix)

            if len(attempted_prefixes) == 1:
                raise RuntimeError("Simulated ingestion failure")

            return IngestionMetrics(
                run_id=dataset_id,
                bucket="raw",
                objects_prefix=prefix,
                objects_saved=1,
            )

        with (
            patch("financial_data.jobs.CbrClient"),
            patch(
                "financial_data.jobs.CbrKeyrateIngestion.run",
                side_effect=simulate_ingestion,
            ) as ingestion_run,
        ):
            # first attempt fails
            with pytest.raises(
                RuntimeError,
                match="Simulated ingestion failure",
            ):
                ingest_dataset(dataset_id, settings)

            with psycopg.connect(TEST_DB_CONN_STR) as conn:
                failed_step = conn.execute(
                    """
                    SELECT status, attempt_id, attempt_count, error
                    FROM metadata.pipeline_steps
                    WHERE pipeline_run_id = %s
                      AND step_name = 'ingestion_keyrate'
                    """,
                    (pipeline_id,),
                ).fetchone()

                unpublished = conn.execute(
                    """
                    SELECT minio_bucket, minio_prefix, objects_saved
                    FROM metadata.dataset_runs
                    WHERE id = %s
                    """,
                    (dataset_id,),
                ).fetchone()

            assert failed_step is not None
            assert failed_step[0] == "error"
            assert failed_step[1] is not None
            assert failed_step[2] == 1
            assert "Simulated ingestion failure" in failed_step[3]
            assert unpublished == (None, None, None)

            # retry succeeds under a new attempt
            result = ingest_dataset(dataset_id, settings)

            with psycopg.connect(TEST_DB_CONN_STR) as conn:
                completed_step = conn.execute(
                    """
                    SELECT status, attempt_id, attempt_count, error
                    FROM metadata.pipeline_steps
                    WHERE pipeline_run_id = %s
                        AND step_name = 'ingestion_keyrate'
                    """,
                    (pipeline_id,),
                ).fetchone()

            assert completed_step is not None
            assert completed_step[0] == "success"
            assert completed_step[1] != failed_step[1]
            assert completed_step[2] == 2
            assert completed_step[3] is None

            assert len(attempted_prefixes) == 2
            assert attempted_prefixes[0] != attempted_prefixes[1]
            assert result.objects_prefix == attempted_prefixes[1]
            assert repo.get_completed_ingestion(dataset_id) == result

            # repeating a completed operation reuses its result
            repeated_result = ingest_dataset(dataset_id, settings)

            assert repeated_result == result
            assert ingestion_run.call_count == 2

    finally:
        with psycopg.connect(TEST_DB_CONN_STR) as conn:
            conn.execute(
                """
                DELETE FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            conn.execute(
                """
                DELETE FROM metadata.dataset_runs
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            conn.execute(
                "DELETE FROM metadata.pipeline_runs WHERE id = %s",
                (pipeline_id,),
            )


@pytest.mark.parametrize("fail_after_write", [False, True])
def test_complete_staging_is_atomic(fail_after_write: bool) -> None:
    repo = MetadataRepository(TEST_DB_CONN_STR)
    pipeline = repo.get_or_create_pipeline_run(
        APP_PIPELINE_NAME,
        f"test-staging-atomic-{uuid4()}"
    )
    pipeline_id = pipeline.pipeline_run_id
    dataset_id = None

    try:
        dataset = repo.prepare_dataset_run(pipeline_id, "keyrate")
        dataset_id = dataset.run_id

        claim = repo.claim_pipeline_step(
            pipeline_id,
            "staging_keyrate",
            "staging",
        )
        assert claim.attempt_id is not None

        # existing rows must survive if replacement fails
        with psycopg.connect(TEST_DB_CONN_STR) as conn:
            conn.execute(
                """
                INSERT INTO stg.keyrate_history
                    (run_id, trade_date, keyrate)
                VALUES (%s, '01.01.2026', '16.00')
                """,
                (dataset_id,),
            )

        loader = KeyrateStagingLoader(
            run_id=dataset_id,
            storage=Mock(spec=MinioStorage),
            bucket="raw",
            prefix="test",
            db_conn_str=TEST_DB_CONN_STR,
        )
        rows = [(dataset_id, "02.01.2026", "16.50")]

        def write_rows(cur):
            result = loader.write_rows(cur, rows)

            if fail_after_write:
                raise RuntimeError("Failure after staging write")

            return result

        if fail_after_write:
            with pytest.raises(
                RuntimeError,
                match="Failure after staging write"
            ):
                repo.complete_staging(
                    claim.step_id,
                    claim.attempt_id,
                    dataset_id,
                    write_rows,
                )

        else:
            result = repo.complete_staging(
                claim.step_id,
                claim.attempt_id,
                dataset_id,
                write_rows,
            )
            assert result.records_loaded == 1

        with psycopg.connect(TEST_DB_CONN_STR) as conn:
            stored_rows = conn.execute(
                """
                SELECT trade_date, keyrate
                FROM stg.keyrate_history
                WHERE run_id = %s
                """,
                (dataset_id,),
            ).fetchall()

            metadata = conn.execute(
                """
                SELECT d.records_loaded, s.status, s.records_out
                FROM metadata.dataset_runs d
                JOIN metadata.pipeline_steps s
                  ON d.pipeline_run_id = s.pipeline_run_id
                 AND s.step_name = 'staging_keyrate'
                WHERE d.id = %s
                """,
                (dataset_id,),
            ).fetchone()

        if fail_after_write:
            assert stored_rows == [("01.01.2026", "16.00")]
            assert metadata == (None, "running", None)
        else:
            assert stored_rows == [("02.01.2026", "16.50")]
            assert metadata == (1, "success", 1)

    finally:
        with psycopg.connect(TEST_DB_CONN_STR) as conn:
            if dataset_id is not None:
                conn.execute(
                    "DELETE FROM stg.keyrate_history WHERE run_id = %s",
                    (dataset_id,),
                )

            conn.execute(
                """
                DELETE FROM metadata.pipeline_steps
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            conn.execute(
                """
                DELETE FROM metadata.dataset_runs
                WHERE pipeline_run_id = %s
                """,
                (pipeline_id,),
            )
            conn.execute(
                "DELETE FROM metadata.pipeline_runs WHERE id = %s",
                (pipeline_id,),
            )