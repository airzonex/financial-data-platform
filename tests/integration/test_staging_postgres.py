import json
from collections.abc import Iterator

import psycopg
import pytest
from config import TEST_DB_CONN_STR

from financial_data.staging import KeyrateStagingLoader, RgbiStagingLoader

BUCKET = 'raw'
RUN_ID = 123
PREFIX_CBR = 'cbr/keyrate/ingestion_date=2026-09-11/run_id=123'
PREFIX_MOEX = 'moex/rgbi/ingestion_date=2026-09-11/run_id=123'


@pytest.fixture(autouse=True)
def clear_staging_tables() -> Iterator[None]:
    """
    фикстура чистит таблицы в stg перед и после каждого теста
    """

    def _truncate_tables():
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


def test_keyrate_load_inserts_rows_into_postgres(
    storage,
) -> None:
    """
    KeyrateStagingLoader.load пишет данные в базу
    """
    object_name = f'{PREFIX_CBR}/part-0000.html'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    <table>
        <tr>
            <th>Date</th>
            <th>Key Rate</th>
        </tr>
        <tr>
            <td>01.09.2026</td>
            <td>15,00</td>
        </tr>
        <tr>
            <td>02.09.2026</td>
            <td>15,50</td>
        </tr>
    </table>
    """

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=TEST_DB_CONN_STR
    )

    result = loader.load()

    assert result.run_id == RUN_ID
    assert result.records_loaded == 2

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, keyrate
                  FROM stg.keyrate_history
                 WHERE run_id = %s
                 ORDER BY trade_date
                """,
            (RUN_ID,)
        )
        rows = cur.fetchall()

    assert rows == [
        (RUN_ID, '01.09.2026', '15,00'),
        (RUN_ID, '02.09.2026', '15,50')
    ]

def test_keyrate_load_is_idempotent(
    storage
) -> None:
    """
    KeyrateStagingLoader.load выполним два раза с одним run_id и проверим что нет дублирования
    """
    object_name = f'{PREFIX_CBR}/part-0000.html'

    storage.list_objects.side_effect = lambda **kwargs: iter([object_name])

    storage.get.return_value = b"""
    <table>
        <tr>
            <th>Date</th>
            <th>Key Rate</th>
        </tr>
        <tr>
            <td>01.09.2026</td>
            <td>15,00</td>
        </tr>
    </table>
    """

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=TEST_DB_CONN_STR
    )

    first_result = loader.load()
    second_result = loader.load()

    assert first_result.records_loaded == 1
    assert second_result.records_loaded == 1

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT COUNT(*)
                  FROM stg.keyrate_history
                 WHERE run_id = %s
                """,
            (RUN_ID,)
        )
        res = cur.fetchone()

    assert res[0] == 1

def test_keyrate_load_does_not_delete_other_run(
    storage
) -> None:
    """
    KeyrateStagingLoader.load не затирает загрузки с другим run_id
    """
    first_run_id = 100
    second_run_id = 200

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                INSERT INTO stg.keyrate_history(
                    run_id,
                    trade_date,
                    keyrate
                )
                VALUES (%s, %s, %s)
                """,
            (first_run_id, '01.09.2026', '15,00')
        )

    object_name = f'{PREFIX_CBR}/part-0000.html'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    <table>
        <tr>
            <th>Date</th>
            <th>Key Rate</th>
        </tr>
        <tr>
            <td>01.09.2026</td>
            <td>15,50</td>
        </tr>
    </table>
    """

    loader = KeyrateStagingLoader(
        run_id=second_run_id,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=TEST_DB_CONN_STR
    )

    loader.load()

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, keyrate
                  FROM stg.keyrate_history
                 ORDER BY run_id
                """
        )
        res = cur.fetchall()

    assert res == [
        (first_run_id, '01.09.2026', '15,00'),
        (second_run_id, '01.09.2026', '15,50')
    ]


def test_rgbi_load_inserts_rows_into_postgres(
    storage
) -> None:
    """
    RgbiStagingLoader.load пишет данные в базу
    """
    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["BOARDID", "SECID", "TRADEDATE", "CLOSE", "CURRENCYID"],
            "data": [
                ["SNDX", "RGBI", "2014-01-04", 100.16, "RUB"],
                ["SNDX", "RGBI", "2014-01-05", 100.88, "RUB"]
            ]
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=TEST_DB_CONN_STR
    )

    result = loader.load()

    assert result.run_id == RUN_ID
    assert result.records_loaded == 2

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, close, currency_id
                  FROM stg.rgbi_history
                 WHERE run_id = %s
                 ORDER BY trade_date
                """,
            (RUN_ID,)
        )
        rows = cur.fetchall()

    assert rows == [
        (RUN_ID, '2014-01-04', '100.16', 'RUB'),
        (RUN_ID, '2014-01-05', '100.88', 'RUB')
    ]


def test_rgbi_load_is_idempotent(
    storage
) -> None:
    """
    RgbiStagingLoader.load выполним два раза с одним run_id и проверим что нет дублирования
    """
    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.side_effect = lambda **kwargs: iter([object_name])

    storage.get.return_value=b"""
    {
        "history": {
            "columns": ["BOARDID", "SECID", "TRADEDATE", "CLOSE", "CURRENCYID"],
            "data": [
                ["SNDX", "RGBI", "2014-01-05", 100.88, "RUB"]
            ]
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=TEST_DB_CONN_STR
    )

    first_result = loader.load()
    second_result = loader.load()

    assert first_result.records_loaded == 1
    assert second_result.records_loaded == 1

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT COUNT(*)
                  FROM stg.rgbi_history
                 WHERE run_id = %s
                """,
            (RUN_ID,)
        )
        res = cur.fetchone()

    assert res[0] == 1

def test_rgbi_load_does_not_delete_other_run(
    storage
) -> None:
    """
    RgbiStagingLoader.load не затирает загрузки с другим run_id
    """
    first_run_id = 100
    second_run_id = 200

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                INSERT INTO stg.rgbi_history(
                    run_id,
                    trade_date,
                    close,
                    currency_id
                )
                VALUES (%s, %s, %s, %s)
                """,
            (first_run_id, '2014-01-05', '100', 'RUB')
        )

    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["BOARDID", "SECID", "TRADEDATE", "CLOSE", "CURRENCYID"],
            "data": [
                ["SNDX", "RGBI", "2014-01-05", 100.5, "RUB"]
            ]
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=second_run_id,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=TEST_DB_CONN_STR
    )

    loader.load()

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT run_id, trade_date, close, currency_id
                  FROM stg.rgbi_history
                 ORDER BY run_id
                """
        )
        res = cur.fetchall()

    assert res == [
        (first_run_id, '2014-01-05', '100', 'RUB'),
        (second_run_id, '2014-01-05', '100.5', 'RUB')
    ]

def test_rgbi_load_does_not_load_rows_if_one_file_is_incorrect(
    storage
) -> None:
    """
    RgbiStagingLoader.load ничего не загрузит в stg.rgbi_history если хоть один файл не корректен
    """
    part0_ok = f'{PREFIX_MOEX}/part-0000.json'
    part1_ok = f'{PREFIX_MOEX}/part-0001.json'
    part2_broken = f'{PREFIX_MOEX}/part-0002.json'

    storage.list_objects.return_value = iter([
        part0_ok,
        part1_ok,
        part2_broken,
    ])

    def _make_content(rows: list) -> bytes:
        return json.dumps({
            'history': {
                'columns': ['BOARDID', 'SECID', 'TRADEDATE', 'CLOSE', 'CURRENCYID'],
                'data': rows
            }
        }).encode()

    data_by_object_name = {
        part0_ok: _make_content([['SNDX', 'RGBI', '2014-01-05', 100.5, 'RUB']]),
        part1_ok: _make_content([['SNDX', 'RGBI', '2014-01-06', 101, 'RUB']]),
        part2_broken: _make_content([])
    }

    def _get_content(**kwargs):
        return data_by_object_name[kwargs['object_name']]

    storage.get.side_effect = _get_content 

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=TEST_DB_CONN_STR
    )

    with pytest.raises(ValueError):
        loader.load()

    with psycopg.connect(TEST_DB_CONN_STR) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT COUNT(*)
                  FROM stg.rgbi_history
                 WHERE run_id = %s
                """,
            (RUN_ID,)
        )
        res = cur.fetchone()

    assert res[0] == 0