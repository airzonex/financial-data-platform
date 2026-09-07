from datetime import datetime, date
from dataclasses import dataclass
import subprocess

from airflow.sdk import dag, task

from financial_data.metadata_repo import MetadataRepository, PipelineInfo, DatasetRunInfo
from financial_data.ingestion import CbrKeyrateIngestion, MoexRgbiIngestion, IngestionMetrics
from financial_data.sources import CbrClient, MoexClient
from financial_data.storage import MinioStorage
from financial_data.staging import KeyrateStagingLoader, RgbiStagingLoader, StagingMetrics
from financial_data.transformation import RgbiWatermarkProvider, KeyrateWatermarkProvider, DatasetRunFinalizer

PIPELINE_NAME = 'financial_data'

RGBI_DATASET = 'rgbi'
RGBI_URL = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RGBI.json'

KEYRATE_DATASET = 'keyrate'
KEYRATE_URL = 'https://www.cbr.ru/hd_base/KeyRate'

PG_CONN_STR = 'postgresql://airflow:airflow@postgres:5432/airflow'

MINIO_ENDPOINT = 'minio:9000'
MINIO_USER = 'minioadmin'
MINIO_PASS = 'minioadmin'
MINIO_RAW_BUCKET = 'raw'


@dag(
    dag_id='financial_data',
    start_date=datetime(2026, 1, 1),
    #schedule='@daily',
    schedule=None,
    catchup=False
)
def financial_data_dag():

    @task
    def create_pipeline_run() -> PipelineInfo:
        repo = MetadataRepository(PG_CONN_STR)

        return repo.create_pipeline_run(PIPELINE_NAME)

    @task
    def prepare_rgbi_run(pipeline_info: PipelineInfo) -> DatasetRunInfo:
        repo = MetadataRepository(PG_CONN_STR)
        
        requested_start = repo.determine_date_start(dataset=RGBI_DATASET)

        run_id = repo.create_dataset_run(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            dataset=RGBI_DATASET,
            requested_start=requested_start
        )

        return DatasetRunInfo(run_id, requested_start, RGBI_DATASET)

    @task
    def prepare_keyrate_run(pipeline_info: PipelineInfo) -> DatasetRunInfo:
        repo = MetadataRepository(PG_CONN_STR)

        requested_start = repo.determine_date_start(dataset=KEYRATE_DATASET)

        run_id = repo.create_dataset_run(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            dataset=KEYRATE_DATASET,
            requested_start=requested_start
        )

        return DatasetRunInfo(run_id, requested_start, KEYRATE_DATASET)

    @task
    def rgbi_ingestion(pipeline_info: PipelineInfo, dataset_run_info: DatasetRunInfo) -> IngestionMetrics:
        repo = MetadataRepository(PG_CONN_STR)

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='ingestion_rgbi',
            step_type='ingestion'
        )

        try:
            moex_client = MoexClient(
                base_url=RGBI_URL,
                date_start=dataset_run_info.requested_start
            )

            storage = MinioStorage(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_USER,
                secret_key=MINIO_PASS
            )

            ingestion = MoexRgbiIngestion(
                client=moex_client,
                storage=storage,
                bucket=MINIO_RAW_BUCKET
            )

            result: IngestionMetrics = ingestion.run(dataset_run_info.run_id, pipeline_info.pipeline_run_date)

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success',
            )

            repo.add_ingestion_metrics(ingestion_result=result)

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
        repo = MetadataRepository(PG_CONN_STR)

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

            storage = MinioStorage(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_USER,
                secret_key=MINIO_PASS
            )

            ingestion = CbrKeyrateIngestion(
                client=cbr_client,
                storage=storage,
                bucket=MINIO_RAW_BUCKET
            )

            result: IngestionMetrics = ingestion.run(
                run_id=dataset_run_info.run_id, 
                run_date=pipeline_info.pipeline_run_date
            )

            repo.finish_pipeline_step(
                step_id=step_id,
                status='success'
            )

            repo.add_ingestion_metrics(ingestion_result=result)

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
        repo = MetadataRepository(PG_CONN_STR)

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='staging_rgbi',
            step_type='staging'
        )

        try:
            storage = MinioStorage(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_USER,
                secret_key=MINIO_PASS
            )

            loader = RgbiStagingLoader(
                run_id=dataset_run_info.run_id,
                storage=storage,
                bucket=ingestion_info.bucket,
                prefix=ingestion_info.objects_prefix,
                db_conn_str=PG_CONN_STR
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
        repo = MetadataRepository(PG_CONN_STR)

        step_id = repo.start_pipeline_step(
            pipeline_run_id=pipeline_info.pipeline_run_id,
            step_name='staging_keyrate',
            step_type='staging'
        )

        try:
            storage = MinioStorage(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_USER,
                secret_key=MINIO_PASS
            )

            loader = KeyrateStagingLoader(
                run_id=dataset_run_info.run_id,
                storage=storage,
                bucket=ingestion_info.bucket,
                prefix=ingestion_info.objects_prefix,
                db_conn_str=PG_CONN_STR
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
        repo = MetadataRepository(PG_CONN_STR)

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

        repo = MetadataRepository(PG_CONN_STR)

        finalizer = DatasetRunFinalizer(
            repository=repo,
            providers={
                'rgbi': RgbiWatermarkProvider(
                    connection_str=PG_CONN_STR,
                    run_id=rgbi_run.run_id
                ),
                'keyrate': KeyrateWatermarkProvider(
                    connection_str=PG_CONN_STR,
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
        repo = MetadataRepository(PG_CONN_STR)

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