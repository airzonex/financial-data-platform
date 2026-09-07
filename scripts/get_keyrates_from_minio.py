from financial_data.staging.keyrate import KeyrateStagingLoader
from financial_data.storage.minio import MinioStorage


pg_conn_str = 'postgresql://airflow:airflow@localhost:5433/airflow'

storage = MinioStorage(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
)

loader = KeyrateStagingLoader(
    run_id=4,
    storage=storage,
    bucket='raw',
    prefix='cbr/keyrate/ingestion_date=2026-09-01/run_id=4',
    db_conn_str=pg_conn_str
)

data = loader.load()
print(data)