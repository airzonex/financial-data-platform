import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from financial_data.ingestion import IngestionMetrics
from financial_data.jobs import (
    build_dbt,
    ingest_dataset,
    stage_dataset,
)
from financial_data.locking import DbtBuildBusyError
from financial_data.metadata_repo import (
    DatasetRunInfo,
    PipelineInfo,
    StepClaim,
)
from financial_data.runtime import RuntimeSettings
from financial_data.staging import StagingMetrics


def test_completed_ingestion_returns_saved_result_without_external_work() -> None:
    settings = Mock(spec=RuntimeSettings)

    saved = IngestionMetrics(
        run_id=2,
        bucket="raw",
        objects_prefix="cbr/keyrate/ingestion_date=2026-11-19/run_id=2",
        objects_saved=1,
    )

    with (
        patch("financial_data.jobs.MetadataRepository") as repository_class,
        patch("financial_data.jobs.CbrClient") as cbr_client,
        patch("financial_data.jobs.MoexClient") as moex_client,
    ):
        repository = repository_class.return_value
        repository.get_ingestion_context.return_value = (
            PipelineInfo(1, date(2026, 11, 19)),
            DatasetRunInfo(2, date(2014, 1, 1), "keyrate"),
        )
        repository.claim_pipeline_step.return_value = StepClaim(
            step_id=1,
            attempt_id=None,
            already_completed=True,
        )
        repository.get_completed_ingestion.return_value = saved

        result = ingest_dataset(2, settings)

        assert result == saved
        settings.create_storage.assert_not_called()
        cbr_client.assert_not_called()
        moex_client.assert_not_called()
        repository.complete_ingestion.assert_not_called()
        repository.finish_claimed_step.assert_not_called()


@pytest.mark.parametrize("dataset", ["keyrate", "rgbi"])
def test_stage_dataset_reuses_completed_result(dataset: str) -> None:
    settings = Mock(spec=RuntimeSettings)
    saved_result = StagingMetrics(
        run_id=2,
        records_loaded=100,
    )

    with (
        patch("financial_data.jobs.MetadataRepository") as repository_class,
        patch("financial_data.jobs.KeyrateStagingLoader") as keyrate_loader,
        patch("financial_data.jobs.RgbiStagingLoader") as rgbi_loader,
    ):
        repository = repository_class.return_value

        repository.get_ingestion_context.return_value = (
            PipelineInfo(1, date(2026, 11, 19)),
            DatasetRunInfo(2, date(2014, 1, 1), dataset),
        )
        repository.claim_pipeline_step.return_value = StepClaim(
            step_id=10,
            attempt_id=None,
            already_completed=True,
        )
        repository.get_completed_staging.return_value = saved_result

        result = stage_dataset(2, settings)

        assert result == saved_result

        repository.get_completed_staging.asset_called_once_with(2)
        repository.get_staging_input.assert_not_called()
        settings.create_storage.assert_not_called()
        keyrate_loader.assert_not_called()
        rgbi_loader.assert_not_called()
        repository.complete_staging.assert_not_called()
        repository.finish_claimed_step.assert_not_called()


def test_build_dbt_reuses_completed_step() -> None:
    settings = Mock(spec=RuntimeSettings)

    with(
        patch("financial_data.jobs.MetadataRepository") as repo_class,
        patch("financial_data.jobs.subprocess.run") as run,
        patch("financial_data.jobs.dbt_build_lock"),
    ):
        repo = repo_class.return_value
        repo.claim_pipeline_step.return_value = StepClaim(
            step_id=10,
            attempt_id=None,
            already_completed=True,
        )

        build_dbt(1, settings)

        repo.claim_pipeline_step.assert_called_once_with(
            pipeline_run_id=1,
            step_name="dbt_build",
            step_type="transformation",
        )
        repo.require_ready_for_dbt.assert_not_called()
        repo.finish_claimed_step.assert_not_called()
        run.assert_not_called()


@pytest.mark.parametrize("build_fails", [False, True])
def test_build_dbt_records_attempt_result(
    build_fails: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # only file existence is checked here; dbt itself is mocked
    (tmp_path / "dbt_project.yml").write_text(
        "name: financial_data\n",
        encoding="utf-8",
    )
    (tmp_path / "profiles.yml").write_text(
        "{}\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("DBT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))

    settings = Mock(spec=RuntimeSettings)
    attempt_id = uuid4()
    failure = subprocess.CalledProcessError(
        returncode=1,
        cmd=["dbt", "build"],
    )

    with (
        patch("financial_data.jobs.MetadataRepository") as repo_class,
        patch("financial_data.jobs.subprocess.run") as run,
        patch("financial_data.jobs.dbt_build_lock"),
    ):
        repo = repo_class.return_value
        repo.claim_pipeline_step.return_value = StepClaim(
            step_id=10,
            attempt_id=attempt_id,
            already_completed=False,
        )

        if build_fails:
            run.side_effect = failure

            with pytest.raises(subprocess.CalledProcessError):
                build_dbt(1, settings)

            repo.finish_claimed_step.assert_called_once_with(
                step_id=10,
                attempt_id=attempt_id,
                status="error",
                error=str(failure),
            )
        else:
            build_dbt(1, settings)

            repo.finish_claimed_step.assert_called_once_with(
                step_id=10,
                attempt_id=attempt_id,
                status="success",
            )

        repo.require_ready_for_dbt.assert_called_once_with(1)

        run.assert_called_once_with(
            [
                "dbt",
                "build",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
            ],
            check=True,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )


def test_build_dbt_does_not_claim_when_lock_is_busy() -> None:
    settings = Mock(spec=RuntimeSettings)

    with (
        patch("financial_data.jobs.MetadataRepository") as repo_class,
        patch("financial_data.jobs.dbt_build_lock") as lock,
        patch("financial_data.jobs.subprocess.run") as run,
    ):
        repo = repo_class.return_value
        lock.return_value.__enter__.side_effect = DbtBuildBusyError(
            "Another financial dbt build holds the lock"
        )

        with pytest.raises(DbtBuildBusyError):
            build_dbt(2, settings)

        repo.require_no_other_unfinalized_dbt_build.assert_not_called()
        repo.claim_pipeline_step.assert_not_called()
        repo.finish_claimed_step.assert_not_called()
        run.assert_not_called()


def test_build_dbt_checks_unfinished_build_before_claiming() -> None:
    settings = Mock(spec=RuntimeSettings)

    with (
        patch("financial_data.jobs.MetadataRepository") as repo_class,
        patch("financial_data.jobs.dbt_build_lock") as lock,
        patch("financial_data.jobs.subprocess.run") as run,
    ):
        repo = repo_class.return_value

        def reject_unfinished_build(pipeline_run_id: int) -> None:
            assert pipeline_run_id == 2
            lock.return_value.__enter__.assert_called_once()
            lock.return_value.__exit__.assert_not_called()
            raise ValueError("Pipeline 1 has an unfinalized dbt build")

        repo.require_no_other_unfinalized_dbt_build.side_effect = (
            reject_unfinished_build
        )

        with pytest.raises(ValueError, match="unfinalized dbt build"):
            build_dbt(2, settings)

        repo.claim_pipeline_step.assert_not_called()
        repo.finish_claimed_step.assert_not_called()
        run.assert_not_called()
        lock.return_value.__exit__.assert_called_once()