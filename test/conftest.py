from unittest.mock import Mock

import pytest
import httpx


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