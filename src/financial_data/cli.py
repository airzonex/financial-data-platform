import argparse
import json
import logging
from dataclasses import asdict

import psycopg

from financial_data.config import (
    KEYRATE_DATASET,
    PIPELINE_NAME,
    RGBI_DATASET,
)
from financial_data.jobs import ingest_dataset
from financial_data.metadata_repo import MetadataRepository
from financial_data.runtime import RuntimeSettings


def check_connections() -> None:
    settings = RuntimeSettings.from_env()

    with (
        psycopg.connect(settings.database_conninfo()) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_user, current_database()")
        user, database = cursor.fetchone()

    print(f"POSTGRES_OK user={user} database={database}", flush=True)

    storage = settings.create_storage()

    # listing is lazy: request one result to perform the actual API call
    # an empty bucket is also a successful result
    next(
        iter(storage.list_objects(bucket="raw", prefix="")),
        None,
    )

    print("MINIO_OK bucket=raw", flush=True)
    print("CONNECTION_CHECK_PASSED", flush=True)


def start_run() -> None:
    settings = RuntimeSettings.from_env()
    repository = MetadataRepository(settings.database_conninfo())

    pipeline = repository.create_pipeline_run(
        pipeline_name=PIPELINE_NAME,
    )

    print(
        json.dumps(
            {
                "pipeline_run_id": pipeline.pipeline_run_id,
                "pipeline_run_date": pipeline.pipeline_run_date.isoformat(),
            }
        )
    )


def prepare_dataset(pipeline_run_id: int, dataset: str) -> None:
    settings = RuntimeSettings.from_env()
    repository = MetadataRepository(settings.database_conninfo())

    result = repository.prepare_dataset_run(
        pipeline_run_id=pipeline_run_id,
        dataset=dataset,
    )

    print(
        json.dumps(
            {
                "pipeline_run_id": pipeline_run_id,
                "dataset_run_id": result.run_id,
                "dataset": result.dataset,
                "requested_start": result.requested_start.isoformat(),
            }
        )
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Financial Data Platform ETL commands"
    )
    commands = parser.add_subparsers(
        dest="command",
        required=True,
    )

    commands.add_parser("check-connections")
    commands.add_parser("start-run")

    prepare_parser = commands.add_parser("prepare-dataset")
    prepare_parser.add_argument(
        "--pipeline-run-id",
        type=int,
        required=True,
    )
    prepare_parser.add_argument(
        "--dataset",
        choices=[RGBI_DATASET, KEYRATE_DATASET],
        required=True,
    )

    ingest_parser = commands.add_parser("ingest")
    ingest_parser.add_argument(
        "--dataset-run-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    if args.command == "check-connections":
        check_connections()
    elif args.command == "start-run":
        start_run()
    elif args.command == "prepare-dataset":
        prepare_dataset(args.pipeline_run_id, args.dataset)
    elif args.command == "ingest":
        result = ingest_dataset(
            dataset_run_id=args.dataset_run_id,
            settings=RuntimeSettings.from_env(),
        )
        print(json.dumps(asdict(result)))


if __name__ == "__main__":
    main()