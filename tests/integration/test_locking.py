import pytest
from config import TEST_DB_CONN_STR

from financial_data.locking import (
    DbtBuildBusyError,
    dbt_build_lock,
)


def test_dbt_build_lock_excludes_another_session() -> None:
    with (
        dbt_build_lock(TEST_DB_CONN_STR),
        pytest.raises(DbtBuildBusyError, match="Another financial dbt build"),
        dbt_build_lock(TEST_DB_CONN_STR),
    ):
        pytest.fail("Second session acquire the held lock")

    # the first session has closed, so acquisition should now succeed
    with dbt_build_lock(TEST_DB_CONN_STR):
        pass


def test_dbt_build_lock_releases_after_exception() -> None:
    with (
        pytest.raises(RuntimeError, match="Simulated build failure"),
        dbt_build_lock(TEST_DB_CONN_STR),
    ):
        raise RuntimeError("Simulated build failure")

    with dbt_build_lock(TEST_DB_CONN_STR):
        pass