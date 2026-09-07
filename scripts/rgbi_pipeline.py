from datetime import date

from financial_data.metadata_repo import MetadataRepository
from financial_data.ingestion import IngestionMetrics, MoexRgbiIngestion
from financial_data.sources import MoexClient
from financial_data.storage import MinioStorage
from financial_data.staging import RgbiStagingLoader, StagingMetrics


RAW_BUCKET = 'raw'

RGBI_URL = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RGBI.json'

pg_conn_str = 'postgresql://airflow:airflow@localhost:5433/airflow'

storage = MinioStorage(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
)

repository = MetadataRepository(pg_conn_str)

date_start = repository.determine_date_start(dataset='rgbi')
run_id, run_date = repository.create_run('moex', 'rgbi', date_start)

moex_client = MoexClient(
    base_url=RGBI_URL,
    date_start=date_start
)

ingestion = MoexRgbiIngestion(
    client=moex_client,
    storage=storage,
    bucket=RAW_BUCKET
)

ingestion_res: IngestionMetrics = ingestion.run(run_id, run_date)

print(f'Saved objects count: {ingestion_res.objects_saved}.\n'
      f'MinIO bucket: {ingestion_res.bucket}\n'
      f'MiniIO prefix: {ingestion_res.objects_prefix}')

repository.mark_objects_loaded(ingestion_res.run_id, 
                               ingestion_res.bucket, 
                               ingestion_res.objects_prefix, 
                               ingestion_res.objects_saved)

rgbi_stage_loader = RgbiStagingLoader(run_id, 
                                      storage, 
                                      ingestion_res.bucket,
                                      ingestion_res.objects_prefix,
                                      pg_conn_str)

metrics: StagingMetrics = rgbi_stage_loader.load()

repository.mark_staging_finished(
    run_id=run_id,
    records_loaded=metrics.records_loaded
)