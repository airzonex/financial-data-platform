from financial_data.metadata_repo import MetadataRepository
from financial_data.sources import CbrClient
from financial_data.ingestion import IngestionMetrics
from financial_data.ingestion import CbrKeyrateIngestion
from financial_data.storage import MinioStorage
from financial_data.staging import KeyrateStagingLoader


KEYRATE_SOURCE = 'cbr'
KEYRATE_DATASET = 'keyrate'

CBR_URL = 'https://www.cbr.ru/hd_base/KeyRate'

RAW_BUCKET = 'raw'

pg_conn_str = 'postgresql://airflow:airflow@localhost:5433/airflow'

repository = MetadataRepository(pg_conn_str)

date_start = repository.determine_date_start(KEYRATE_DATASET)
run_id, run_date = repository.create_run(KEYRATE_SOURCE, KEYRATE_DATASET, date_start)

storage = MinioStorage(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
)

cbr_client = CbrClient(
    CBR_URL,
    date_start
)

ingestion = CbrKeyrateIngestion(
    cbr_client,
    storage,
    RAW_BUCKET
)

ingestion_result: IngestionMetrics = ingestion.run(run_id, run_date)

print(f'{ingestion_result.run_id}\n{ingestion_result.bucket}\n{ingestion_result.objects_prefix}')

repository.mark_objects_loaded(
    ingestion_result.run_id,
    ingestion_result.bucket, 
    ingestion_result.objects_prefix, 
    ingestion_result.objects_saved
)

keyrate_stage_loader = KeyrateStagingLoader(
    run_id, 
    storage, 
    ingestion_result.bucket,
    ingestion_result.objects_prefix,
    pg_conn_str
)

keyrate_stage_loader.load()