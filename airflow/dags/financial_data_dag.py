import subprocess
from datetime import UTC, datetime

import httpx
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook

from financial_data.config import *
from financial_data.ingestion import (
    CbrKeyrateIngestion,
    IngestionMetrics,
    MoexRgbiIngestion,
)
from financial_data.metadata_repo import (
    DatasetRunInfo,
    MetadataRepository,
    PipelineInfo,
)
from financial_data.sources import CbrClient, MoexClient
from financial_data.staging import (
    KeyrateStagingLoader,
    RgbiStagingLoader,
    StagingMetrics,
)
from financial_data.storage import MinioStorage
from financial_data.transformation import (
    DatasetRunFinalizer,
    KeyrateWatermarkProvider,
    RgbiWatermarkProvider,
)

POSTGRES_CONN_ID = 'financial_postgres'
MINIO_CONN_ID = 'financial_minio'


@dag(
    dag_id='financial_data',
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule='@daily',
    catchup=False
)
def financial_data_dag():

    def get_metadata_repository() -> MetadataRepository:
        conn = BaseHook.get_connection(POSTGRES_CONN_ID)

        return MetadataRepository(conn.get_uri())

    def get_db_connection_str() -> str:
        conn = BaseHook.get_connection(POSTGRES_CONN_ID)

        return conn.get_uri()

    def get_minio_storage() -> MinioStorage:
        conn = BaseHook.get_connection(MINIO_CONN_ID)

        return MinioStorage(
            endpoint=conn.host + ':' + str(conn.port),
            access_key=conn.login,
            secret_key=conn.password
        )


    @task
    def create_pipeline_run() -> PipelineInfo:

        return get_metadata_repository().create_pipeline_run(PIPELINE_NAME)


    @task
    def prepare_rgbi_run(pipeline_info: PipelineInfo) -> DatasetRunInfo:
        repo = get_metadata_repository()
        
        requested_start = repo.determine_date_start(dataset=RGBI_DATASET)

        run_id = repo.create_dataset_run(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            dataset=RGBI_DATASET,
            requested_start=requested_start
        )

        return DatasetRunInfo(run_id, requested_start, RGBI_DATASET)

    @task
    def prepare_keyrate_run(pipeline_info: PipelineInfo) -> DatasetRunInfo:
        repo = get_metadata_repository()

        requested_start = repo.determine_date_start(dataset=KEYRATE_DATASET)

        run_id = repo.create_dataset_run(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            dataset=KEYRATE_DATASET,
            requested_start=requested_start
        )

        return DatasetRunInfo(run_id, requested_start, KEYRATE_DATASET)

    @task
    def rgbi_ingestion(pipeline_info: PipelineInfo, dataset_run_info: DatasetRunInfo) -> IngestionMetrics:
        repo = get_metadata_repository()

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='ingestion_rgbi',
            step_type='ingestion'
        )

        try:
            storage = get_minio_storage()

            with httpx.Client(timeout=30.0) as http_client:
                moex_client = MoexClient(
                    base_url=RGBI_URL,
                    date_start=dataset_run_info.requested_start,
                    http_client=http_client
                )

                ingestion = MoexRgbiIngestion(
                    client=moex_client,
                    storage=storage,
                    bucket=MINIO_RAW_BUCKET
                )

                result: IngestionMetrics = ingestion.run(dataset_run_info.run_id, pipeline_info.pipeline_run_date)

            repo.add_ingestion_metrics(ingestion_result=result)

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success',
            )

            return result

        except Exception as e:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='error',
                error=str(e)
            )
            raise

    @task
    def keyrate_ingestion(pipeline_info: PipelineInfo, dataset_run_info: DatasetRunInfo) -> IngestionMetrics:
        repo = get_metadata_repository()

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='ingestion_keyrate',
            step_type='ingestion'
        )

        try:

            cbr_client = CbrClient(
                base_url=KEYRATE_URL,
                date_start=dataset_run_info.requested_start
            )

            storage = get_minio_storage()

            ingestion = CbrKeyrateIngestion(
                client=cbr_client,
                storage=storage,
                bucket=MINIO_RAW_BUCKET
            )

            result: IngestionMetrics = ingestion.run(
                run_id=dataset_run_info.run_id, 
                run_date=pipeline_info.pipeline_run_date
            )

            repo.add_ingestion_metrics(ingestion_result=result)

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success'
            )

            return result

        except Exception as e:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='error',
                error=str(e)
            )
            raise

    @task
    def rgbi_staging(
            pipeline_info: PipelineInfo, 
            dataset_run_info: DatasetRunInfo,
            ingestion_info: IngestionMetrics
        ) -> StagingMetrics:
        repo = get_metadata_repository()

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='staging_rgbi',
            step_type='staging'
        )

        try:
            storage = get_minio_storage()

            loader = RgbiStagingLoader(
                run_id=dataset_run_info.run_id,
                storage=storage,
                bucket=ingestion_info.bucket,
                prefix=ingestion_info.objects_prefix,
                db_conn_str=get_db_connection_str()
            )

            result: StagingMetrics = loader.load()

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success',
                records_out=result.records_loaded
            )

            repo.add_staging_metrics(staging_result=result)

            return result

        except Exception as e:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='error',
                error=str(e)
            )
            raise

    @task
    def keyrate_staging(pipeline_info: PipelineInfo, 
                        dataset_run_info: DatasetRunInfo, 
                        ingestion_info: IngestionMetrics) -> StagingMetrics:
        repo = get_metadata_repository()

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='staging_keyrate',
            step_type='staging'
        )

        try:
            storage = get_minio_storage()

            loader = KeyrateStagingLoader(
                run_id=dataset_run_info.run_id,
                storage=storage,
                bucket=ingestion_info.bucket,
                prefix=ingestion_info.objects_prefix,
                db_conn_str=get_db_connection_str()
            )

            result: StagingMetrics = loader.load()

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success',
                records_out=result.records_loaded
            )

            repo.add_staging_metrics(staging_result=result)

            return result

        except Exception as e:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='error',
                error=str(e)
            )
            raise


    @task
    def dbt_build(pipeline_info: PipelineInfo) -> None:
        repo = get_metadata_repository()

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='dbt_build',
            step_type='transformation'
        )

        try:
            subprocess.run(
                [
                    'dbt',
                    'build',
                    '--project-dir',
                    '/opt/airflow/dbt/financial_data'
                ],
                check=True
            )

        except Exception as e:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='error',
                error=str(e)
            )
            raise

        else:
            repo.finish_pipeline_step(
                step_id=step_id,
                status='success',
            )

    @task
    def finish_dataset_runs(rgbi_run: DatasetRunInfo, keyrate_run: DatasetRunInfo) -> None:

        repo = get_metadata_repository()

        finalizer = DatasetRunFinalizer(
            repository=repo,
            providers={
                'rgbi': RgbiWatermarkProvider(
                    connection_str=get_db_connection_str(),
                    run_id=rgbi_run.run_id
                ),
                'keyrate': KeyrateWatermarkProvider(
                    connection_str=get_db_connection_str(),
                    run_id=keyrate_run.run_id
                )
            }
        )

        finalizer.finalize(
            dataset_runs={
                'rgbi': rgbi_run.run_id,
                'keyrate': keyrate_run.run_id
            }
        )
        

    @task(trigger_rule='all_done')
    def finish_pipeline_run(pipeline_info: PipelineInfo) -> None:
        repo = get_metadata_repository()

        repo.finish_pipeline_run(pipeline_info.pipeline_run_id)

    # --------------------------------------------------------------------------------
    # DAG graph
    # --------------------------------------------------------------------------------

    pipeline: PipelineInfo = create_pipeline_run()

    rgbi_run: DatasetRunInfo = prepare_rgbi_run(pipeline)
    keyrate_run: DatasetRunInfo = prepare_keyrate_run(pipeline)

    rgbi_ingest: IngestionMetrics = rgbi_ingestion(pipeline_info=pipeline, dataset_run_info=rgbi_run)
    keyrate_ingest: IngestionMetrics = keyrate_ingestion(
        pipeline_info=pipeline,
        dataset_run_info=keyrate_run
    )

    rgbi_stage: StagingMetrics = rgbi_staging(
        pipeline_info=pipeline,
        dataset_run_info=rgbi_run,
        ingestion_info=rgbi_ingest
    )
    keyrate_stage: StagingMetrics = keyrate_staging(
        pipeline_info=pipeline,
        dataset_run_info=keyrate_run,
        ingestion_info=keyrate_ingest
    )

    dbt = dbt_build(pipeline)

    [rgbi_stage, keyrate_stage] >> dbt

    finish_runs = finish_dataset_runs(rgbi_run, keyrate_run)
    dbt >> finish_runs

    finish_pipeline = finish_pipeline_run(pipeline)
    finish_runs >> finish_pipeline


dag = financial_data_dag()