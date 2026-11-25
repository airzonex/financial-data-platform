from datetime import date
from unittest.mock import Mock, patch

from financial_data.ingestion import IngestionMetrics
from financial_data.jobs import ingest_dataset
from financial_data.metadata_repo import (
    DatasetRunInfo,
    PipelineInfo,
    StepClaim,
)
from financial_data.runtime import RuntimeSettings


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