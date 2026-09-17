from unittest.mock import patch

import pytest

from financial_data.staging import KeyrateStagingLoader, RgbiStagingLoader


RUN_ID = 123
BUCKET = 'raw'
PREFIX_CBR = 'cbr/keyrate/ingestion_date=2026-09-11/run_id=123'
PREFIX_MOEX = 'moex/rgbi/ingestion_date=2026-09-11/run_id=123'
DB_CONN_STR = 'postgresql://test'


def test_keyrate_get_single_object_name_raises_when_no_objects(
    storage
) -> None:
    """
    KeyrateStagingLoader._get_single_object_name()
    Нет объектов
    """
    storage.list_objects.return_value = iter(())

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    with pytest.raises(
        FileNotFoundError,
        match=f'No objects found under {BUCKET}/{PREFIX_CBR}'
    ):
        loader._get_single_object_name()

def test_keyrate_get_single_object_name_returns_object(
    storage
) -> None:
    """
    KeyrateStagingLoader._get_single_object_name()
    Один объект
    """
    storage.list_objects.return_value = iter(
        [f'{PREFIX_CBR}/part-0000.html']
    )

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    result = loader._get_single_object_name()

    assert result == f'{PREFIX_CBR}/part-0000.html'

def test_keyrate_get_single_object_name_raises_when_multiple_objects(
    storage
) -> None:
    """
    KeyrateStagingLoader._get_single_object_name()
    Больше одного объекта
    """
    storage.list_objects.return_value = iter(
        [
            f'{PREFIX_CBR}/part-0000.html',
            f'{PREFIX_CBR}/part-0001.html'
        ]
    )

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    with pytest.raises(
        ValueError,
        match=f'Expected exactly one object under {BUCKET}/{PREFIX_CBR}'
    ):
        loader._get_single_object_name()

def test_keyrate_parse_raw_bytes_returns_rows(
    storage
) -> None:
    """
    KeyrateStagingLoader._parse_raw_bytes()
    Корректный HTML
    """
    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    raw = b"""
    <html>
        <body>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Key Rate</th>
                </tr>
                <tr>
                    <td>01.01.2026</td>
                    <td>16.00</td>
                </tr>
                <tr>
                    <td>02.01.2026</td>
                    <td>16.50</td>
                </tr>
            </table>
        </body>
    </html>
    """

    result = loader._parse_raw_bytes(raw)

    assert result == (
        (RUN_ID, '01.01.2026', '16.00'),
        (RUN_ID, '02.01.2026', '16.50')
    )

def test_keyrate_parse_raw_bytes_raises_when_table_tag_missing(
    storage
) -> None:
    """
    KeyrateStagingLoader._parse_raw_bytes()
    Нет <table>
    """
    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    with pytest.raises(
        ValueError,
        match='Tag table not found in soup'
    ):
        loader._parse_raw_bytes(b'<html><body>text</body></html>')

def test_keyrate_parse_raw_bytes_raises_when_table_is_empty(
    storage
) -> None:
    """
    KeyrateStagingLoader._parse_raw_bytes()
    Пустая таблица    
    """
    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    raw =  b"""
    <html>
        <body>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Key Rate</th>
                </tr>
            </table>
        </body>
    </html>
    """

    with pytest.raises(
        ValueError,
        match='Table is empty or has an invalid format'
    ):
        loader._parse_raw_bytes(raw)

def test_keyrate_load_reads_objects_and_inserts_rows(
    storage,
    db_connection
) -> None:
    """
    KeyrateStagingLoader.load()
    Читает объекты и вставляет строки
    """
    conn, cursor = db_connection

    storage.list_objects.return_value = iter(
        [f'{PREFIX_CBR}/part-0000.html']
    )

    storage.get.return_value = b"""
    <html>
        <body>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Key Rate</th>
                </tr>
                <tr>
                    <td>01.01.2026</td>
                    <td>16.00</td>
                </tr>
                <tr>
                    <td>02.01.2026</td>
                    <td>16.50</td>
                </tr>
            </table>
        </body>
    </html>
    """

    cursor.fetchone.return_value = (2,)

    loader = KeyrateStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_CBR,
        db_conn_str=DB_CONN_STR
    )

    with patch(
        'financial_data.staging.psycopg.connect',
        return_value=conn
    ):
        result = loader.load()

    storage.list_objects.assert_called_once_with(
        bucket=BUCKET,
        prefix=PREFIX_CBR
    )

    storage.get.assert_called_once_with(
        bucket=BUCKET,
        object_name=f'{PREFIX_CBR}/part-0000.html'
    )

    cursor.executemany.assert_called_once()

    inserted_rows = cursor.executemany.call_args.args[1]

    assert inserted_rows == (
        (RUN_ID, '01.01.2026', '16.00'),
        (RUN_ID, '02.01.2026', '16.50')
    )

    assert result.run_id == RUN_ID
    assert result.records_loaded == 2

def test_rgbi_reads_json_and_inserts_rows(
    storage,
    db_connection
) -> None:
    """
    RgbiStagingLoader
    Читает и загружает
    """    
    conn, cursor = db_connection

    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["BOARDID", "SECID", "TRADEDATE", "CLOSE", "CURRENCYID"], 
            "data": [
                ["SNDX", "RGBI", "2002-12-30", 100, "RUB"],
                ["SNDX", "RGBI", "2003-01-04", 100.16, "RUB"]
            ]
        }
    }
    """

    cursor.fetchone.return_value = (2,)

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=DB_CONN_STR
    )

    with patch(
        'financial_data.staging.psycopg.connect',
        return_value=conn
    ):
        result = loader.load()

    storage.list_objects.assert_called_once_with(
        bucket=BUCKET,
        prefix=PREFIX_MOEX
    )

    storage.get.assert_called_once_with(
        bucket=BUCKET,
        object_name=object_name
    )

    cursor.executemany.assert_called_once()

    inserted_rows = cursor.executemany.call_args.args[1]

    assert inserted_rows == [
        (RUN_ID, '2002-12-30', 100, 'RUB'),
        (RUN_ID, '2003-01-04', 100.16, 'RUB'),
    ]

    assert result.run_id == RUN_ID
    assert result.records_loaded == 2

def test_rgbi_load_not_json_objects_are_ignored(
    storage,
    db_connection
) -> None:
    """
    RgbiStagingLoader
    He-json объекты игнорируются
    """
    conn, cursor = db_connection

    json_object = f'{PREFIX_MOEX}/part-0000.json'
    html_object = f'{PREFIX_MOEX}/part-0001.html'

    storage.list_objects.return_value = iter(
        [html_object, json_object]
    )

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["TRADEDATE", "CLOSE", "CURRENCYID"], 
            "data": [
                ["2002-12-30", 100, "RUB"]
            ]
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=DB_CONN_STR
    )

    with patch(
        'financial_data.staging.psycopg.connect',
        return_value=conn
    ):
        loader.load()

    storage.get.assert_called_once_with(
        bucket=BUCKET,
        object_name=json_object
    )

def test_rgbi_load_raises_when_no_needed_column(
    storage,
    db_connection
) -> None:
    """
    RgbiStagingLoader.load()
    В ответе сервера нет нужной колонки
    """
    conn, _ = db_connection

    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["TRADEDATE", "CURRENCYID"], 
            "data": [
                ["2002-12-30", "RUB"]
            ]
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=DB_CONN_STR
    )

    with patch('financial_data.staging.psycopg.connect', return_value=conn):
        with pytest.raises(ValueError, match="'CLOSE' is not in list"):
            loader.load()

def test_rgbi_load_raises_when_data_is_empty(
    storage,
    db_connection
) -> None:
    """
    RgbiStagingLoader.load()
    В ответе сервера history.data пустой
    """
    conn, _ = db_connection

    object_name = f'{PREFIX_MOEX}/part-0000.json'

    storage.list_objects.return_value = iter([object_name])

    storage.get.return_value = b"""
    {
        "history": {
            "columns": ["TRADEDATE", "CLOSE", "CURRENCYID"], 
            "data": []
        }
    }
    """

    loader = RgbiStagingLoader(
        run_id=RUN_ID,
        storage=storage,
        bucket=BUCKET,
        prefix=PREFIX_MOEX,
        db_conn_str=DB_CONN_STR
    )

    with patch('financial_data.staging.psycopg.connect', return_value=conn):
        with pytest.raises(ValueError, match="history.data is empty"):
            loader.load()
