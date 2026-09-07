from financial_data.storage.minio import MinioStorage
from financial_data.staging.rgbi import RgbiStagingLoader


run_id = 2
bucket = 'raw'
objects_prefix = 'moex/rgbi/ingestion_date=2026-08-28/run_id=2'
pg_conn_str = 'postgresql://airflow:airflow@localhost:5433/airflow'

storage = MinioStorage(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
)

rgbi_stage_loader = RgbiStagingLoader(run_id, 
                                      storage, 
                                      bucket,
                                      objects_prefix,
                                      pg_conn_str)

rgbi_stage_loader.load()