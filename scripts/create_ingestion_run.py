from financial_data.metadata.repository import MetadataRepository


conn_str = 'postgresql://airflow:airflow@localhost:5433/airflow'

repository = MetadataRepository(conn_str)
print(repository.get_last_successful_date(dataset='rgbi'))
date_start = repository.determine_date_start(dataset='rgbi')
run = repository.create_run('moex', 'rgbi', date_start)
print(run)