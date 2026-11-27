import logging
import os
import subprocess
import sys
from collections.abc import Callable
from functools import partial
from pathlib import Path

import httpx
import psycopg

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

    pipeline, dataset_run = repository.get_ingestion_context(
        dataset_run_id,
        allow_completed=True,
    )

    if dataset_run.dataset not in (KEYRATE_DATASET, RGBI_DATASET):
        raise ValueError(f"Unsupported dataset: {dataset_run.dataset}")

    claim = repository.claim_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name=f"ingestion_{dataset_run.dataset}",
        step_type="ingestion",
    )

    if claim.already_completed:
        result = repository.get_completed_ingestion(dataset_run_id)

        logger.info(
            "Reusing completed ingestion: dataset_run_id=%s",
            dataset_run_id,
        )

        return result

    attempt_id = claim.attempt_id

    if attempt_id is None:
        raise RuntimeError("An executable claim must have an attempt ID")

    try:
        storage = settings.create_storage()

        logger.info(
            "Starting ingestion: dataset=%s dataset_run_id=%s "
            "start=%s attempt_id=%s",
            dataset_run.dataset,
            dataset_run.run_id,
            dataset_run.requested_start,
            attempt_id,
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
                attempt_id=attempt_id,
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
                    attempt_id=attempt_id,
                )

        repository.complete_ingestion(
            step_id=claim.step_id,
            attempt_id=attempt_id,
            result=result,
        )

    except Exception as exc:
        try:
            repository.finish_claimed_step(
                step_id=claim.step_id,
                attempt_id=attempt_id,
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

    pipeline, dataset_run = repository.get_ingestion_context(
        dataset_run_id,
        allow_completed=True,
    )

    if dataset_run.dataset not in (KEYRATE_DATASET, RGBI_DATASET):
        raise ValueError(
            f"Unsupported dataset: {dataset_run.dataset}"
        )

    claim = repository.claim_pipeline_step(
        pipeline_run_id=pipeline.pipeline_run_id,
        step_name=f"staging_{dataset_run.dataset}",
        step_type="staging",
    )

    if claim.already_completed:
        result = repository.get_completed_staging(dataset_run_id)

        logger.info(
            "Reusing completed staging: dataset_run_id=%s",
            dataset_run_id,
        )

        return result

    attempt_id = claim.attempt_id

    if attempt_id is None:
        raise RuntimeError(
            "An executable claim must have an attempt ID"
        )

    try:
        ingestion = repository.get_staging_input(dataset_run_id)
        storage = settings.create_storage()

        logger.info(
            "Starting staging: dataset=%s dataset_run_id=%s attempt_id=%s",
            dataset_run.dataset,
            dataset_run_id,
            attempt_id,
        )

        writer: Callable[[psycopg.Cursor], StagingMetrics]

        if dataset_run.dataset == KEYRATE_DATASET:
            keyrate_loader = KeyrateStagingLoader(
                run_id=dataset_run_id,
                storage=storage,
                bucket=ingestion.bucket,
                prefix=ingestion.objects_prefix,
                db_conn_str=connection_str,
            )
            keyrate_rows = keyrate_loader.read_rows()
            writer = partial(
                keyrate_loader.write_rows,
                rows=keyrate_rows,
            )
        else:
            rgbi_loader = RgbiStagingLoader(
                run_id=dataset_run_id,
                storage=storage,
                bucket=ingestion.bucket,
                prefix=ingestion.objects_prefix,
                db_conn_str=connection_str,
            )
            rgbi_rows = rgbi_loader.read_rows()
            writer = partial(
                rgbi_loader.write_rows,
                rows=rgbi_rows,
            )

        result = repository.complete_staging(
            step_id=claim.step_id,
            attempt_id=attempt_id,
            dataset_run_id=dataset_run_id,
            write_rows=writer,
        )

    except Exception as exc:
        try:
            repository.finish_claimed_step(
                step_id=claim.step_id,
                attempt_id=attempt_id,
                status="error",
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "Could not record staging failure"
            )

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
    repository = MetadataRepository(settings.database_conninfo())

    claim = repository.claim_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        step_name="dbt_build",
        step_type="transformation",
    )

    if claim.already_completed:
        logger.info(
            "Reusing completed dbt build: pipeline_run_id=%s",
            pipeline_run_id,
        )
        return

    attempt_id = claim.attempt_id

    if attempt_id is None:
        raise RuntimeError(
            "An executable claim must have an attempt ID"
        )

    try:
        repository.require_ready_for_dbt(pipeline_run_id)

        project_dir = Path(os.environ["DBT_PROJECT_DIR"])
        profiles_dir = Path(os.environ["DBT_PROFILES_DIR"])

        for required_file in (
            project_dir / "dbt_project.yml",
            profiles_dir / "profiles.yml",
        ):
            if not required_file.is_file():
                raise FileNotFoundError(f"Missing dbt file {required_file}")

        logger.info(
            "Starting dbt build: pipeline_run_id=%s attempt_id=%s",
            pipeline_run_id,
            attempt_id,
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

        repository.finish_claimed_step(
            step_id=claim.step_id,
            attempt_id=attempt_id,
            status="success",
        )

    except Exception as exc:
        try:
            repository.finish_claimed_step(
                step_id=claim.step_id,
                attempt_id=attempt_id,
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