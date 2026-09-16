from unittest.mock import Mock, MagicMock

import pytest
import httpx

from financial_data.storage import MinioStorage
from financial_data.sources import CbrClient, MoexClient
from financial_data.ingestion import MoexPage


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