from pathlib import Path
from unittest.mock import MagicMock, Mock

import httpx
import psycopg
import pytest
from config import ADMIN_DB_CONN_STR, TEST_DB_CONN_STR, TEST_DB_NAME

from financial_data.ingestion import MoexPage
from financial_data.sources import CbrClient, MoexClient
from financial_data.storage import MinioStorage

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA_FILES = [
    PROJECT_ROOT / 'postgres' / 'init' / '01-init.sql',
    PROJECT_ROOT / 'postgres' / 'init' / '02-metadata.sql',
    PROJECT_ROOT / 'postgres' / 'init' / '03-stg.sql',
]

@pytest.fixture
def cbr_mock_response():
    def factory(
        *,
        content: bytes = b''
    ) -> Mock:
        response = Mock(spec=httpx.Response)
        response.content = content
        response.raise_for_status.return_value = None
        return response

    return factory

@pytest.fixture
def moex_mock_response():
    def factory(
        *,
        content: bytes = b'',
        json_data: dict | None = None
    ) -> Mock:
        response = Mock(spec=httpx.Response)
        response.content = content
        response.json.return_value = json_data if json_data is not None else {}
        response.raise_for_status.return_value = None
        return response

    return factory

@pytest.fixture
def http_client():
    return Mock(spec=httpx.Client)

@pytest.fixture
def storage():
    return Mock(spec=MinioStorage)

@pytest.fixture
def cbr_client():
    return Mock(spec=CbrClient)

@pytest.fixture
def moex_client() -> Mock:
    return Mock(spec=MoexClient)

@pytest.fixture
def moex_page():
    def factory(
        *,
        raw: bytes,
        index: int,
        total: int,
        page_size: int
    ) -> MoexPage:
        return MoexPage(
            raw=raw,
            data={
                "history.cursor": {
                    "data": [[index, total, page_size]]
                }
            }
        )

    return factory

@pytest.fixture
def db_connection():
    conn = MagicMock()
    cursor = MagicMock()

    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None

    return conn, cursor

@pytest.fixture(scope='session', autouse=True)
def ensure_test_database() -> None:
    """
    Creates test database if not exists
    """
    with psycopg.connect(ADMIN_DB_CONN_STR, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            'SELECT 1 FROM pg_database WHERE datname = %s',
            (TEST_DB_NAME,)
        )
        db_exists = cur.fetchone() is not None

        if not db_exists:
            cur.execute(f'CREATE DATABASE "{TEST_DB_NAME}"')

    _init_test_db()

def _init_test_db() -> None:
    with psycopg.connect(TEST_DB_CONN_STR, autocommit=True) as conn, conn.cursor() as cur:
        for file_path in SCHEMA_FILES:
            if not file_path.exists():
                raise FileNotFoundError(f'Schema file not found: {file_path}')
            sql = file_path.read_text()
            cur.execute(sql)