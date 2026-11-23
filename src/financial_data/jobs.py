import logging

import httpx

from financial_data.config import (
    KEYRATE_DATASET,
    KEYRATE_URL,
    MINIO_RAW_BUCKET,
    RGBI_DATASET,
    RGBI_URL,
)
from financial_data.ingestion import (
    CbrKeyrateIngestion,
    IngestionMetrics,
    MoexRgbiIngestion,
)
from financial_data.metadata_repo import MetadataRepository
from financial_data.runtime import RuntimeSettings
from financial_data.sources import CbrClient, MoexClient

logger = logging.getLogger(__name__)


def ingest_dataset(
    dataset_run_id: int,
    settings: RuntimeSettings
) -> IngestionMetrics:
    repository = MetadataRepository(settings.database_conninfo())
    pipeline, dataset_run = repository.get_ingestion_context(dataset_run_id)

    if dataset_run.dataset not in (KEYRATE_DATASET, RGBI_DATASET):
        raise ValueError(f"Unsupported dataset: {dataset_run.dataset}")

    storage = settings.create_storage()

    step_id = repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name=f"ingestion_{dataset_run.dataset}",
        step_type="ingestion",
    )

    try:
        logger.info(
            "Starting ingestion: dataset=%s dataset_run_id=%s start=%s",
            dataset_run.dataset,
            dataset_run.run_id,
            dataset_run.requested_start,
        )

        if dataset_run.dataset == KEYRATE_DATASET:
            cbr_client = CbrClient(
                base_url=KEYRATE_URL,
                date_start=dataset_run.requested_start,
            )
            keyrate_ingestion = CbrKeyrateIngestion(
                client=cbr_client,
                storage=storage,
                bucket=MINIO_RAW_BUCKET,
            )
            result = keyrate_ingestion.run(
                run_id=dataset_run.run_id,
                run_date=pipeline.pipeline_run_date,
            )
        else:
            with httpx.Client(timeout=30.0) as http_client:
                moex_client = MoexClient(
                    base_url=RGBI_URL,
                    date_start=dataset_run.requested_start,
                    http_client=http_client,
                )
                rgbi_ingestion = MoexRgbiIngestion(
                    client=moex_client,
                    storage=storage,
                    bucket=MINIO_RAW_BUCKET,
                )
                result = rgbi_ingestion.run(
                    run_id=dataset_run.run_id,
                    run_date=pipeline.pipeline_run_date,
                )

        repository.add_ingestion_metrics(result)
        repository.finish_pipeline_step(
            step_id=step_id,
            status="success",
        )

    except Exception as exc:
        try:
            repository.finish_pipeline_step(
                step_id=step_id,
                status="error",
                error=str(exc)
            )
        except Exception:
            logger.exception("Could not record ingestion failure")

        raise

    logger.info(
        "Ingestion finished: dataset_run_id=%s objects_saved=%s",
        result.run_id,
        result.objects_saved,
    )

    return result