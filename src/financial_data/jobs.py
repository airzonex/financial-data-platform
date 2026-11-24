import logging
import os
import subprocess
import sys
from pathlib import Path

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
from financial_data.staging import (
    KeyrateStagingLoader,
    RgbiStagingLoader,
    StagingMetrics,
)

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


def stage_dataset(
    dataset_run_id: int,
    settings: RuntimeSettings,
) -> StagingMetrics:
    connection_str = settings.database_conninfo()
    repository = MetadataRepository(connection_str)

    # this lookup also checks that both runs are still running
    pipeline, dataset_run = repository.get_ingestion_context(dataset_run_id)
    ingestion = repository.get_staging_input(dataset_run_id)

    loader_class: type[KeyrateStagingLoader | RgbiStagingLoader]

    if dataset_run.dataset == KEYRATE_DATASET:
        loader_class = KeyrateStagingLoader
    elif dataset_run.dataset == RGBI_DATASET:
        loader_class = RgbiStagingLoader
    else:
        raise ValueError(f"Unsupported dataset {dataset_run_id}")

    loader = loader_class(
        run_id=dataset_run_id,
        storage=settings.create_storage(),
        bucket=ingestion.bucket,
        prefix=ingestion.objects_prefix,
        db_conn_str=connection_str,
    )

    step_id = repository.start_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name=f"staging_{dataset_run.dataset}",
        step_type="staging",
    )

    try:
        logger.info(
            "Starting staging: dataset=%s dataset_run_id=%s",
            dataset_run.dataset,
            dataset_run_id,
        )

        result = loader.load()

        if result.records_loaded <= 0:
            raise ValueError(f"No staging rows loaded for dataset run {dataset_run_id}")

        repository.add_staging_metrics(result)
        repository.finish_pipeline_step(
            step_id=step_id,
            status="success",
            records_out=result.records_loaded,
        )
    except Exception as exc:
        try:
            repository.finish_pipeline_step(
                step_id=step_id,
                status="error",
                error=str(exc),
            )
        except Exception:
            logger.exception("Could not record staging failure")

        raise

    logger.info(
        "Staging finished: dataset_run_id=%s records_loaded=%s",
        dataset_run_id,
        result.records_loaded,
    )

    return result


def build_dbt(
    pipeline_run_id: int,
    settings: RuntimeSettings,
) -> None:
    project_dir = Path(os.environ["DBT_PROJECT_DIR"])
    profiles_dir = Path(os.environ["DBT_PROFILES_DIR"])

    for required_file in (
        project_dir / "dbt_project.yml",
        profiles_dir / "profiles.yml",
    ):
        if not required_file.is_file():
            raise FileNotFoundError(f"Missing dbt file {required_file}")

    repository = MetadataRepository(settings.database_conninfo())
    repository.require_ready_for_dbt(pipeline_run_id)

    step_id = repository.start_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        step_name="dbt_build",
        step_type="transformation",
    )

    try:
        logger.info(
            "Starting dbt build: pipeline_run_id=%s",
            pipeline_run_id,
        )

        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(profiles_dir),
            ],
            check=True,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )

        repository.finish_pipeline_step(
            step_id=step_id,
            status="success",
        )
    except Exception as exc:
        try:
            repository.finish_pipeline_step(
                step_id=step_id,
                status="error",
                error=str(exc),
            )
        except Exception:
            logger.exception("Could not record dbt failure")

        raise

    logger.info(
        "dbt build finished: pipeline_run_id=%s",
        pipeline_run_id,
    )


def finalize_pipeline(
    pipeline_run_id: int,
    settings: RuntimeSettings,
) -> dict[str, str]:
    repository = MetadataRepository(settings.database_conninfo())

    watermarks = repository.finalize_successful_pipeline(pipeline_run_id)

    logger.info(
        "Pipeline finalized: pipeline_run_id=%s",
        pipeline_run_id,
    )

    return {
        dataset: watermark.isoformat()
        for dataset, watermark in watermarks.items()
    }