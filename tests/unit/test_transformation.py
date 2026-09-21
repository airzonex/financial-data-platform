from datetime import date
from unittest.mock import Mock, call, patch

import pytest

from financial_data.transformation import (
    DatasetRunFinalizer,
    KeyrateWatermarkProvider,
    RgbiWatermarkProvider,
)

DB_CONN_STR = 'postgresql://test'
RUN_ID = 123


# def test_rgbi_watermark_provider_returns_date(
#     db_connection
# ) -> None:
#     """
#     RgbiWatermarkProvider возвращает дату
#     """
#     conn, cursor = db_connection

#     expected_date = date(2014, 1, 1)
#     cursor.fetchone.return_value = (expected_date, )

#     provider = RgbiWatermarkProvider(
#         connection_str=DB_CONN_STR,
#         run_id=RUN_ID
#     )

#     with patch(
#         'financial_data.transformation.psycopg.connect',
#         return_value=conn
#     ):
#         result = provider.get_max_date()

#     assert result == expected_date

# def test_rgbi_watermark_provider_return_none_if_no_data(
#     db_connection
# ) -> None:
#     """
#     RgbiWatermarkProvider возвращает None если данных в таблице нет
#     """
#     conn, cursor = db_connection

#     cursor.fetchone.return_value = (None,)

#     provider = RgbiWatermarkProvider(
#         connection_str=DB_CONN_STR,
#         run_id=RUN_ID
#     )

#     with patch(
#         'financial_data.transformation.psycopg.connect',
#         return_value=conn
#     ):
#         result = provider.get_max_date()

#     assert result is None

# def test_keyrate_watermark_provider_returns_date(
#     db_connection
# ) -> None:
#     """
#     KeyrateWatermarkProvider возвращает дату
#     """
#     conn, cursor = db_connection

#     expected_date = date(2014, 1, 1)
#     cursor.fetchone.return_value = (expected_date,)

#     provider = KeyrateWatermarkProvider(
#         connection_str=DB_CONN_STR,
#         run_id=RUN_ID
#     )

#     with patch(
#         'financial_data.transformation.psycopg.connect',
#         return_value=conn
#     ):
#         result = provider.get_max_date()

#     assert result == expected_date

# def test_keyrate_watermark_provider_return_none_if_no_data(
#     db_connection
# ) -> None:
#     """
#     KeyrateWatermarkProvider возвращает None если данных в таблице нет
#     """
#     conn, cursor = db_connection

#     cursor.fetchone.return_value = (None,)

#     provider = KeyrateWatermarkProvider(
#         connection_str=DB_CONN_STR,
#         run_id=RUN_ID
#     )

#     with patch(
#         'financial_data.transformation.psycopg.connect',
#         return_value=conn
#     ):
#         result = provider.get_max_date()

#     assert result is None

@pytest.mark.parametrize(
    ('provider_class', 'expected_value'),
    [
        (RgbiWatermarkProvider, date(2014, 1, 1)),
        (RgbiWatermarkProvider, None),
        (KeyrateWatermarkProvider, date(2014, 1, 1)),
        (KeyrateWatermarkProvider, None)
    ]
)
def test_watermark_provider_returns_expected_value(
    db_connection,
    provider_class,
    expected_value
) -> None:
    """
    параметризованный тест
    тестирует два класса RgbiWatermarkProvider и KeyrateWatermarkProvider
    1. возвращают корректную дату
    2. возвращают None если база ничего не находит
    """
    conn, cursor = db_connection

    cursor.fetchone.return_value = (expected_value,)

    provider = provider_class(
        connection_str=DB_CONN_STR,
        run_id=RUN_ID
    )

    with patch(
        'financial_data.transformation.psycopg.connect',
        return_value=conn
    ):
        result = provider.get_max_date()

    assert result == expected_value

def test_dataset_run_finalizer_finishes_all_datasets() -> None:
    """
    DatasetRunFinalizer.finalize отправляет корректные данные в 
    repository.finish_dataset_run в finalize
    """
    repo = Mock()

    keyrate_provider = Mock()
    keyrate_provider_max_date = date(2026, 9, 1)
    keyrate_provider.get_max_date.return_value = keyrate_provider_max_date

    rgbi_provider = Mock()
    rgbi_provider_max_date = date(2026, 9, 2)
    rgbi_provider.get_max_date.return_value = rgbi_provider_max_date

    finalizer = DatasetRunFinalizer(
        repository=repo,
        providers={
            'keyrate': keyrate_provider,
            'rgbi': rgbi_provider
        }
    )

    finalizer.finalize(
        dataset_runs={
            'keyrate': 101,
            'rgbi': 102
        }
    )

    keyrate_provider.get_max_date.assert_called_once()
    rgbi_provider.get_max_date.assert_called_once()

    assert repo.finish_dataset_run.call_args_list == [
        call(101, keyrate_provider_max_date),
        call(102, rgbi_provider_max_date)
    ]

def test_dataset_run_finalizer_raises_when_run_id_is_missing() -> None:
    """
    DatasetRunFinalizer.finalize вызывает ValueError если dataset'a нет в dataset_runs
    """
    repo = Mock()

    rgbi_provider = Mock()
    rgbi_provider.get_max_date.return_value = date(2026, 9, 1)

    finalizer = DatasetRunFinalizer(
        repository=repo,
        providers={
            'rgbi': rgbi_provider
        }
    )

    with pytest.raises(
        ValueError,
        match='Dataset run id is missing for dataset rgbi'
    ):
        finalizer.finalize(
            dataset_runs={}
        )

    rgbi_provider.get_max_date.assert_not_called()
    repo.finish_dataset_run.assert_not_called()

def test_dataset_run_finalizer_raises_when_no_data() -> None:
    """
    DatasetRunFinalizer.finalize вызывает ValueError когда provider возвращает None
    """
    repo = Mock()

    rgbi_provider = Mock()
    rgbi_provider.get_max_date.return_value = None

    finalizer = DatasetRunFinalizer(
        repository=repo,
        providers={
            'rgbi': rgbi_provider
        }
    )

    with pytest.raises(
        ValueError,
        match='No data found for rgbi dataset, run_id = 101'
    ):
        finalizer.finalize(
            dataset_runs={
                'rgbi': 101
            }
        )

    rgbi_provider.get_max_date.assert_called_once()
    repo.finish_dataset_run.assert_not_called()