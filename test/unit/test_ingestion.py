"""
MOEX
- правильный start
- правильная пагинация
- правильный part_number
- каждый raw response сохраняется
- последний page не приводит к лишнему запросу
- total == 0 → ничего не сохраняем
- page_size <= 0 → ошибка

CBR
- один запрос
- один raw object
- правильный object name
- ошибка клиента не проглатывается
"""

from unittest.mock import call
from datetime import date

import pytest

from financial_data.ingestion import CbrKeyrateIngestion, MoexRgbiIngestion, IngestionMetrics, MoexPage


BUCKET = 'raw'
RUN_ID = 123
RUN_DATE = date(2026, 9, 11)


def test_cbr_ingestion_fetches_and_saves_page(
    storage,
    cbr_client    
) -> None:
    """
    Проверяем сразу весь контракт CbrKeyrateIngestion: один вызов источника, правильный путь, 
    сохранение именно bytes, content_type и метрики.
    """
    raw = b'<html>CBR data</html>'

    client = cbr_client
    client.fetch_cbr_page.return_value = raw

    ingestion = CbrKeyrateIngestion(
        client=client,
        storage=storage,
        bucket=BUCKET
    )

    result = ingestion.run(
        run_id=RUN_ID,
        run_date=RUN_DATE
    )

    client.fetch_cbr_page.assert_called_once_with()

    storage.put.assert_called_once_with(
        bucket=BUCKET,
        object_name=(
            'cbr/keyrate/'
            'ingestion_date=2026-09-11/'
            f'run_id={RUN_ID}/'
            'part-0000.html'
        ),
        data=raw,
        content_type='text/html'
    )

    assert result == IngestionMetrics(
        run_id=RUN_ID,
        bucket=BUCKET,
        objects_prefix=(
            'cbr/keyrate/'
            'ingestion_date=2026-09-11/'
            f'run_id={RUN_ID}'
        ),
        objects_saved=1
    )

def test_cbr_ingestion_propagates_client_error(
    cbr_client,
    storage
) -> None:
    client = cbr_client
    client.fetch_cbr_page.side_effect = RuntimeError('CBR error')

    ingestion = CbrKeyrateIngestion(
        client=client,
        storage=storage,
        bucket=BUCKET
    )

    with pytest.raises(RuntimeError, match='CBR error'):
        ingestion.run(
            run_id=RUN_ID,
            run_date=RUN_DATE
        )

    storage.put.assert_not_called()
    
def test_moex_ingestion_saves_single_page(
    moex_client,
    storage
) -> None:
    raw = b'{"history": "..."}'

    page = MoexPage(
        raw=raw,
        data={"history.cursor": {
            "data": [
                [0, 50, 100]
            ]
        }
        }
    )

    moex_client.fetch_rgbi_page.return_value = page

    ingestion = MoexRgbiIngestion(
        client=moex_client,
        storage=storage,
        bucket=BUCKET
    )

    result = ingestion.run(
        run_id=RUN_ID,
        run_date=RUN_DATE
    )

    moex_client.fetch_rgbi_page.assert_called_once_with(
        start=0
    )

    storage.put.assert_called_once_with(
        bucket=BUCKET,
        object_name=(
            'moex/rgbi/'
            'ingestion_date=2026-09-11/'
            f'run_id={RUN_ID}/'
            'part-0000.json'
        ),
        data=raw,
        content_type='application/json'
    )

    assert result == IngestionMetrics(
        run_id=RUN_ID,
        bucket=BUCKET,
        objects_prefix=(
            'moex/rgbi/'
            'ingestion_date=2026-09-11/'
            f'run_id={RUN_ID}'
        ),
        objects_saved=1
    )

def test_moex_ingestion_paginates_and_saves_all_pages(
    moex_client,
    storage,
    moex_page
) -> None:
    """
    допустим client возвращает три страницы:
    page 1: index=0,   total=250, page_size=100
    page 2: index=100, total=250, page_size=100
    page 3: index=200, total=250, page_size=100
    """
    moex_client.fetch_rgbi_page.side_effect = [
        moex_page(raw=b'page1', index=0, total=250, page_size=100),
        moex_page(raw=b'page2', index=100, total=250, page_size=100),
        moex_page(raw=b'page3', index=200, total=250, page_size=100)
    ]

    ingestion = MoexRgbiIngestion(
        client=moex_client,
        storage=storage,
        bucket=BUCKET
    )

    result = ingestion.run(
        run_id=RUN_ID,
        run_date=RUN_DATE
    )

    assert moex_client.fetch_rgbi_page.call_args_list == [
        call(start=0),
        call(start=100),
        call(start=200)
    ]

    assert storage.put.call_count == 3

    assert storage.put.call_args_list == [
        call(
            bucket=BUCKET,
            object_name=(
                'moex/rgbi/'
                'ingestion_date=2026-09-11/'
                f'run_id={RUN_ID}/'
                'part-0000.json'
            ),
            data=b'page1',
            content_type='application/json'
        ),
        call(
            bucket=BUCKET,
            object_name=(
                'moex/rgbi/'
                'ingestion_date=2026-09-11/'
                f'run_id={RUN_ID}/'
                'part-0001.json'
            ),
            data=b'page2',
            content_type='application/json'
        ),
        call(
            bucket=BUCKET,
            object_name=(
                'moex/rgbi/'
                'ingestion_date=2026-09-11/'
                f'run_id={RUN_ID}/'
                'part-0002.json'
            ),
            data=b'page3',
            content_type='application/json'
        ),
    ]

    assert result.objects_saved == 3

def test_moex_ingestion_does_not_save_empty_result(
    moex_client,
    moex_page,
    storage
) -> None:
    page = moex_page(
        raw=b'page',
        index=0,
        total=0,
        page_size=100
    )

    moex_client.fetch_rgbi_page.return_value = page

    ingestion = MoexRgbiIngestion(
        client=moex_client,
        storage=storage,
        bucket=BUCKET
    )

    result = ingestion.run(
        run_id=RUN_ID,
        run_date=RUN_DATE
    )

    moex_client.fetch_rgbi_page.assert_called_once_with(start=0)
    storage.put.assert_not_called()

    assert result.objects_saved == 0

def test_moex_ingestion_rejects_invalid_page_size(
    moex_client,
    moex_page,
    storage
) -> None:
    page = moex_page(
        raw=b'page',
        index=0,
        total=100,
        page_size=0
    )

    moex_client.fetch_rgbi_page.return_value = page

    ingestion = MoexRgbiIngestion(
        client=moex_client,
        storage=storage,
        bucket=BUCKET
    )

    with pytest.raises(ValueError, match='Invalid page size received from MOEX: 0'):
        ingestion.run(
            run_id=RUN_ID,
            run_date=RUN_DATE
        )

    storage.put.assert_not_called()

def test_moex_ingestion_propagates_client_error(
    moex_client,
    storage
) -> None:
    moex_client.fetch_rgbi_page.side_effect = RuntimeError('MOEX error')

    ingestion = MoexRgbiIngestion(
        client=moex_client,
        storage=storage,
        bucket=BUCKET
    )

    with pytest.raises(RuntimeError, match='MOEX error'):
        ingestion.run(
            run_id=RUN_ID,
            run_date=RUN_DATE
        )

    storage.put.assert_not_called()


    