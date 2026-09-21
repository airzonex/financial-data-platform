from datetime import date
from unittest.mock import Mock, patch

import httpx
import pytest

from financial_data.sources import CbrClient, MoexClient

CBR_URL = 'https://example.com/cbr'
MOEX_URL = 'https://example.com/moex'


def test_cbr_client_send_correct_date_parameter(cbr_mock_response) -> None:
    client = CbrClient(
        base_url=CBR_URL,
        date_start=date(2014, 1, 5)
    )

    response = cbr_mock_response(
        content=b'<html>test</html>'
    )

    with patch(
        'financial_data.sources.httpx.get',
        return_value=response
    ) as mock_get:
        result = client.fetch_cbr_page()

    mock_get.assert_called_once_with(
        CBR_URL,
        params={
            'UniDbQuery.Posted': 'True',
            'UniDbQuery.From': '05.01.2014'
        },
        follow_redirects=True,
        timeout=10
    )

    assert result == b'<html>test</html>'

def test_cbr_client_raises_http_error(cbr_mock_response) -> None:
    client = CbrClient(
        base_url=CBR_URL,
        date_start=date(2014, 1, 1)
    )

    response = cbr_mock_response()

    error = httpx.HTTPStatusError(
        '500 Server Error',
        request=Mock(spec=httpx.Request),
        response=response
    )

    response.raise_for_status.side_effect = error

    with patch(
        'financial_data.sources.httpx.get',
        return_value=response
    ), pytest.raises(httpx.HTTPStatusError):
        client.fetch_cbr_page()

    response.raise_for_status.assert_called_once()

def test_cbr_client_returns_raw_response_bytes(cbr_mock_response) -> None:
    client = CbrClient(
        base_url=CBR_URL,
        date_start=date(2014, 1, 1)
    )

    raw_content = b'\xff\xfe\x00\x01'

    response = cbr_mock_response(
        content=raw_content
    )

    with patch(
        'financial_data.sources.httpx.get',
        return_value=response
    ):
        result = client.fetch_cbr_page()

    assert type(result) is bytes
    assert result == raw_content

@pytest.mark.parametrize(
    ('input_date', 'expected'),
    [
        (date(2014, 1, 1), '01.01.2014'),
        (date(2020, 12, 31), '31.12.2020'),
        (date(2026, 9, 8), '08.09.2026')
    ]
)
def test_cbr_client_formats_date_correctly(
    cbr_mock_response,
    input_date: date,
    expected: str
) -> None:
    client = CbrClient(
        base_url=CBR_URL,
        date_start=input_date
    )

    response = cbr_mock_response()

    with patch(
        'financial_data.sources.httpx.get',
        return_value=response
    ) as mock_get:
        client.fetch_cbr_page()

    params = mock_get.call_args.kwargs['params']

    assert params['UniDbQuery.From'] == expected

def test_moex_client_sends_required_parameters(
    http_client,
    moex_mock_response
) -> None:
    http_client.get.return_value = moex_mock_response()

    client = MoexClient(
        base_url=MOEX_URL,
        date_start=date(2014, 1, 1),
        http_client=http_client
    )

    client.fetch_rgbi_page()

    http_client.get.assert_called_once_with(
        MOEX_URL,
        params={
            'from': '2014-01-01',
            'start': 0
        }
    )

def test_moex_client_sends_start_parameter(
    http_client,
    moex_mock_response
) -> None:
    http_client.get.return_value = moex_mock_response()

    client = MoexClient(
        base_url=MOEX_URL,
        date_start=date(2014, 1, 1),
        http_client=http_client
    )

    client.fetch_rgbi_page(
        start=200
    )

    http_client.get.assert_called_once_with(
        MOEX_URL,
        params={
            'from': '2014-01-01',
            'start': 200
        }
    )

def test_moex_client_adds_limit_when_provided(
    http_client,
    moex_mock_response
) -> None:
    http_client.get.return_value = moex_mock_response()

    client = MoexClient(
        base_url=MOEX_URL,
        date_start=date(2014, 1, 1),
        http_client=http_client
    )

    client.fetch_rgbi_page(
        start=100,
        limit=50
    )

    http_client.get.assert_called_once_with(
        MOEX_URL,
        params={
            'from': '2014-01-01',
            'start': 100,
            'limit': 50
        }
    )

def test_moex_client_raises_http_error(
    http_client,
    moex_mock_response
) -> None:
    response = moex_mock_response()
    error = httpx.HTTPStatusError(
        '500 Server Error',
        request=Mock(spec=httpx.Request),
        response=response
    )

    response.raise_for_status.side_effect = error
    http_client.get.return_value = response

    client = MoexClient(
        base_url=MOEX_URL,
        date_start=date(2026, 1, 1),
        http_client=http_client
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_rgbi_page()

    response.raise_for_status.assert_called_once()

def test_moex_client_returns_raw_and_parsed_data(
    http_client,
    moex_mock_response
) -> None:
    raw = b'{"foo": "bar"}'
    data = {'foo': 'bar'}

    response = moex_mock_response(
        content=raw,
        json_data=data
    )

    http_client.get.return_value = response

    client = MoexClient(
        base_url=MOEX_URL,
        date_start=date(2026, 1, 1),
        http_client=http_client
    )

    result = client.fetch_rgbi_page()

    assert result.raw == raw
    assert result.data == data