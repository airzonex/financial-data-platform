import json
import subprocess
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import psycopg
import pytest

from financial_data.ingestion import CbrKeyrateIngestion, MoexRgbiIngestion
from financial_data.metadata_repo import MetadataRepository
from financial_data.sources import MoexPage
from financial_data.staging import KeyrateStagingLoader, RgbiStagingLoader
from financial_data.storage import MinioStorage
from financial_data.transformation import (
    DatasetRunFinalizer,
    KeyrateWatermarkProvider,
    RgbiWatermarkProvider,
)
from minio import Minio

TEST_DB_CONN_STR = (
    'postgresql://airflow:airflow@localhost:5433/financial_test'
)

MINIO_ENDPOINT = 'localhost:9000'
MINIO_USER = 'minioadmin'
MINIO_PASS = 'minioadmin'
MINIO_BUCKET = 'test'

PIPELINE_NAME = 'financial_data_test'

KEYRATE_DATASET = 'keyrate'
RGBI_DATASET = 'rgbi'

DBT_PROJECT_DIR = (
    Path(__file__).resolve().parents[2]
    / 'dbt'
    / 'financial_data'
)


@pytest.fixture
def e2e_cbr_client(
    cbr_client
) -> Mock:
    cbr_client.fetch_cbr_page.return_value = b"""
    <html>
        <body>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Key Rate</th>
                </tr>
                <tr>
                    <td>01.01.2026</td>
                    <td>16,00</td>
                </tr>
                <tr>
                    <td>02.01.2026</td>
                    <td>16,50</td>
                </tr>
                <tr>
                    <td>03.01.2026</td>
                    <td>17,00</td>
                </tr>
            </table>
        </body>
    </html>
    """
    return cbr_client

@pytest.fixture
def e2e_moex_client(
    moex_client
) -> Mock:

    def _get_rgbi_page(**kwargs) -> MoexPage:
        start = kwargs['start']

        trade_date = f"2025-06-0{start + 1}"
        close = 100 + start
        index = start

        data = {
            "history": {
                "columns": ["BOARDID", "SECID", "TRADEDATE", "CLOSE", "CURRENCYID"], 
                "data": [
                    ["SNDX", "RGBI", trade_date, close, "RUB"],
                ]
            },
            "history.cursor": {
	            "columns": ["INDEX", "TOTAL", "PAGESIZE"], 
	            "data": [
		            [index, 3, 1]
	            ]
            }
        }

        raw = json.dumps(data).encode()

        return MoexPage(
            raw=raw,
            data=data
        )

    moex_client.fetch_rgbi_page.side_effect = _get_rgbi_page

    return moex_client

@pytest.fixture
def e2e_storage() -> MinioStorage:
    return MinioStorage(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS
    )

@pytest.fixture
def e2e_repository() -> MetadataRepository:
    return MetadataRepository(TEST_DB_CONN_STR)

@pytest.fixture(scope='session')
def e2e_minio_client() -> Minio:
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client

@pytest.fixture(autouse=True)
def cleanup_test_objects(
    e2e_minio_client: Minio,
) -> Iterator[None]:
    yield

    for object in e2e_minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        recursive=True
    ):
        e2e_minio_client.remove_object(
            bucket_name=MINIO_BUCKET,
            object_name=object.object_name
        )

@pytest.fixture(autouse=True)
def clear_repository() -> Iterator[None]:

    def _truncata_tables() -> None:
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

    _truncata_tables()
    yield
    _truncata_tables()

@pytest.fixture(autouse=True)
def clear_staging() -> Iterator[None]:

    def _truncate_tables() -> None:
        with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
            cur.execute(
                """
                    TRUNCATE
                        stg.rgbi_history,
                        stg.keyrate_history
                    """
            )

    _truncate_tables()
    yield
    _truncate_tables()

@pytest.fixture
def dbt_profiles_dir(tmp_path: Path) -> Path:
    profiles_dir = tmp_path / 'dbt'
    profiles_dir.mkdir()

    profiles_file = profiles_dir / 'profiles.yml'

    profiles_file.write_text(
        """
financial_data:
  target: test
  outputs:
    test:
      type: postgres
      host: localhost
      port: 5433
      user: airflow
      password: airflow
      dbname: financial_test
      schema: public
      threads: 4
""".strip()
    )
    return profiles_dir

    
def test_keyrate_pipeline_end_to_end(
    e2e_repository: MetadataRepository,
    e2e_cbr_client: Mock,
    e2e_storage: MinioStorage,
    dbt_profiles_dir: Path
) -> None:
    """
    End to end keyrate pipeline happy path
    """
    pipeline = e2e_repository.create_pipeline_run(PIPELINE_NAME)

    requested_start = e2e_repository.determine_date_start(KEYRATE_DATASET)

    run_id = e2e_repository.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset=KEYRATE_DATASET,
        requested_start=requested_start
    )

    ing_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='ingestion_keyrate',
        step_type='ingestion'
    )

    ingestion = CbrKeyrateIngestion(
        client=e2e_cbr_client,
        storage=e2e_storage,
        bucket=MINIO_BUCKET
    )

    ing_metrics = ingestion.run(
        run_id=run_id,
        run_date=pipeline.pipeline_run_date
    )

    e2e_repository.finish_pipeline_step(
        step_id=ing_step_id,
        status='success'
    )

    e2e_repository.add_ingestion_metrics(ing_metrics)

    objects = list(
        e2e_storage.list_objects(
            bucket=MINIO_BUCKET,
            prefix=ing_metrics.objects_prefix
        )
    )

    assert objects == [f'{ing_metrics.objects_prefix}/part-0000.html']

    stg_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='staging_keyrate',
        step_type='staging'
    )

    loader = KeyrateStagingLoader(
        run_id=run_id,
        storage=e2e_storage,
        bucket=ing_metrics.bucket,
        prefix=ing_metrics.objects_prefix,
        db_conn_str=TEST_DB_CONN_STR
    )

    stg_metrics = loader.load()

    e2e_repository.finish_pipeline_step(
        step_id=stg_step_id,
        status='success',
        records_out=stg_metrics.records_loaded
    )

    e2e_repository.add_staging_metrics(staging_result=stg_metrics)

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, keyrate
                  FROM stg.keyrate_history
                 ORDER BY trade_date
                """
        )
        rows = cur.fetchall()

    assert rows == [
        (run_id, '01.01.2026', '16,00'),
        (run_id, '02.01.2026', '16,50'),
        (run_id, '03.01.2026', '17,00')
    ]

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.executemany(
            """
                INSERT INTO stg.rgbi_history(
                    run_id,
                    trade_date,
                    close,
                    currency_id)
                VALUES (%s, %s, %s, %s)
                """,
            [
                (999999, '2026-01-01', '100', 'RUB'),
                (999999, '2026-01-02', '101', 'RUB'),
                (999999, '2026-01-03', '102', 'RUB'),
            ]
        )

    dbt_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='dbt_build',
        step_type='transformation'
    )

    subprocess.run(
        [
            'dbt',
            'build',
            '--project-dir',
            str(DBT_PROJECT_DIR),
            '--profiles-dir',
            str(dbt_profiles_dir)
        ],
        check=True
    )

    e2e_repository.finish_pipeline_step(
        step_id=dbt_step_id,
        status='success',
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT source_run_id, trade_date, keyrate
                  FROM int.keyrate_history
                 ORDER BY trade_date
                """
        )
        int_rows = cur.fetchall()

        cur.execute(
            """
                SELECT trade_date, rgbi_close, keyrate
                  FROM marts.rgbi_keyrate
                 ORDER BY trade_date
                """
        )
        mart_rows = cur.fetchall()

    assert int_rows == [
        (run_id, date(2026, 1, 1), 16.00),
        (run_id, date(2026, 1, 2), 16.50),
        (run_id, date(2026, 1, 3), 17.00),
    ]

    assert mart_rows == [
        (date(2026, 1, 1), 100, 16.00),
        (date(2026, 1, 2), 101, 16.50),
        (date(2026, 1, 3), 102, 17.00),
    ]

    finalizer = DatasetRunFinalizer(
        repository=e2e_repository,
        providers={
            'keyrate': KeyrateWatermarkProvider(
                connection_str=TEST_DB_CONN_STR,
                run_id=run_id
            )
        }
    )

    finalizer.finalize(
        dataset_runs={
            'keyrate': run_id
        }
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT actual_max_date
                  FROM metadata.dataset_runs
                 WHERE id = %s
                """,
            (run_id,)
        )
        max_date = cur.fetchone()[0]

    assert max_date == date(2026, 1, 3)

    e2e_repository.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT status, finished_at
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
            (pipeline.pipeline_run_id,)
        )
        status, finished_at = cur.fetchone()[:2]

    assert status == 'success'
    assert finished_at is not None

def test_rgbi_pipeline_end_to_end(
    e2e_repository: MetadataRepository,
    e2e_moex_client: Mock,
    e2e_storage: MinioStorage,
    dbt_profiles_dir: Path
) -> None:
    """
    End to end rgbi pipeline happy path
    """
    pipeline = e2e_repository.create_pipeline_run(PIPELINE_NAME)

    requested_start = e2e_repository.determine_date_start(RGBI_DATASET)

    run_id = e2e_repository.create_dataset_run(
        pipeline_run_id=pipeline.pipeline_run_id,
        dataset=RGBI_DATASET,
        requested_start=requested_start
    )

    ing_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='ingestion_rgbi',
        step_type='ingestion'
    )

    ingestion = MoexRgbiIngestion(
        client=e2e_moex_client,
        storage=e2e_storage,
        bucket=MINIO_BUCKET
    )

    ing_metrics = ingestion.run(
        run_id=run_id,
        run_date=pipeline.pipeline_run_date
    )

    e2e_repository.finish_pipeline_step(
        step_id=ing_step_id,
        status='success'
    )

    e2e_repository.add_ingestion_metrics(ing_metrics)

    objects = list(
        e2e_storage.list_objects(
            bucket=MINIO_BUCKET,
            prefix=ing_metrics.objects_prefix
        )
    )

    assert objects == [
        f'{ing_metrics.objects_prefix}/part-0000.json',
        f'{ing_metrics.objects_prefix}/part-0001.json',
        f'{ing_metrics.objects_prefix}/part-0002.json',
    ]

    stg_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='staging_rgbi',
        step_type='staging'
    )

    loader = RgbiStagingLoader(
        run_id=run_id,
        storage=e2e_storage,
        bucket=ing_metrics.bucket,
        prefix=ing_metrics.objects_prefix,
        db_conn_str=TEST_DB_CONN_STR
    )

    stg_metrics = loader.load()

    e2e_repository.finish_pipeline_step(
        step_id=stg_step_id,
        status='success',
        records_out=stg_metrics.records_loaded
    )

    e2e_repository.add_staging_metrics(staging_result=stg_metrics)

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, close, currency_id
                  FROM stg.rgbi_history
                 ORDER BY trade_date
                """
        )
        rows = cur.fetchall()

    assert rows == [
        (run_id, '2025-06-01', '100', 'RUB'),
        (run_id, '2025-06-02', '101', 'RUB'),
        (run_id, '2025-06-03', '102', 'RUB')
    ]

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.executemany(
            """
                INSERT INTO stg.keyrate_history(
                    run_id,
                    trade_date,
                    keyrate)
                VALUES (%s, %s, %s)
                """,
            [
                (999999, '01.06.2025', '16,00'),
                (999999, '02.06.2025', '16,50'),
                (999999, '03.06.2025', '17,00'),
            ]
        )

    dbt_step_id = e2e_repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name='dbt_build',
        step_type='transformation'
    )

    subprocess.run(
        [
            'dbt',
            'build',
            '--project-dir',
            str(DBT_PROJECT_DIR),
            '--profiles-dir',
            str(dbt_profiles_dir)
        ],
        check=True
    )

    e2e_repository.finish_pipeline_step(
        step_id=dbt_step_id,
        status='success',
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT source_run_id, trade_date, rgbi_close, currency_id
                  FROM int.rgbi_history
                 ORDER BY trade_date
                """
        )
        int_rows = cur.fetchall()

        cur.execute(
            """
                SELECT trade_date, rgbi_close, keyrate
                  FROM marts.rgbi_keyrate
                 ORDER BY trade_date
                """
        )
        mart_rows = cur.fetchall()

    assert int_rows == [
        (run_id, date(2025, 6, 1), 100, 'RUB'),
        (run_id, date(2025, 6, 2), 101, 'RUB'),
        (run_id, date(2025, 6, 3), 102, 'RUB'),
    ]

    assert mart_rows == [
        (date(2025, 6, 1), 100, 16.00),
        (date(2025, 6, 2), 101, 16.50),
        (date(2025, 6, 3), 102, 17.00),
    ]

    finalizer = DatasetRunFinalizer(
        repository=e2e_repository,
        providers={
            'rgbi': RgbiWatermarkProvider(
                connection_str=TEST_DB_CONN_STR,
                run_id=run_id
            )
        }
    )

    finalizer.finalize(
        dataset_runs={
            'rgbi': run_id
        }
    )

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT actual_max_date
                  FROM metadata.dataset_runs
                 WHERE id = %s
                """,
            (run_id,)
        )
        max_date = cur.fetchone()[0]

    assert max_date == date(2025, 6, 3)

    e2e_repository.finish_pipeline_run(pipeline.pipeline_run_id)

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT status, finished_at
                  FROM metadata.pipeline_runs
                 WHERE id = %s
                """,
            (pipeline.pipeline_run_id,)
        )
        status, finished_at = cur.fetchone()[:2]

    assert status == 'success'
    assert finished_at is not None
