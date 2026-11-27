from datetime import date
from unittest.mock import Mock, patch

import pytest
from config import TEST_DB_CONN_STR

from financial_data.jobs import finalize_pipeline
from financial_data.locking import (
    DbtBuildBusyError,
    dbt_build_lock,
)
from financial_data.runtime import RuntimeSettings


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


def test_finalize_pipeline_rejects_busy_lock() -> None:
    settings = Mock(spec=RuntimeSettings)
    settings.database_conninfo.return_value = TEST_DB_CONN_STR

    with patch(
        "financial_data.jobs.MetadataRepository"
        ".finalize_successful_pipeline"
    ) as finalize:
        with (
            dbt_build_lock(TEST_DB_CONN_STR),
            pytest.raises(DbtBuildBusyError),
        ):
            finalize_pipeline(1, settings)

        finalize.assert_not_called()


@pytest.mark.parametrize("finalization_fails", [False, True])
def test_finalize_pipeline_holds_and_releases_lock(
    finalization_fails: bool,
) -> None:
    settings = Mock(spec=RuntimeSettings)
    settings.database_conninfo.return_value = TEST_DB_CONN_STR

    watermarks = {
        "rgbi": date(2026, 11, 20),
        "keyrate": date(2026, 11, 23),
    }

    def simulate_finalization(
        pipeline_run_id: int,
    ) -> dict[str, date]:
        assert pipeline_run_id == 1

        # finalization must hold the lock while this method executes
        with (
            pytest.raises(DbtBuildBusyError),
            dbt_build_lock(TEST_DB_CONN_STR),
        ):
            pytest.fail("Finalization ran without holding the lock")

        if finalization_fails:
            raise ValueError("Simulated finalization failure")

        return watermarks

    with patch(
        "financial_data.jobs.MetadataRepository"
        ".finalize_successful_pipeline",
        side_effect=simulate_finalization,
    ) as finalize:
        if finalization_fails:
            with pytest.raises(
                ValueError,
                match="Simulated finalization failure",
            ):
                finalize_pipeline(1, settings)
        else:
            result = finalize_pipeline(1, settings)

            assert result == {
                "rgbi": "2026-11-20",
                "keyrate": "2026-11-23",
            }

        finalize.assert_called_once_with(1)

    # a new session can acquire it after either outcome
    with dbt_build_lock(TEST_DB_CONN_STR):
        pass