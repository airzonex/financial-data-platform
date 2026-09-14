import psycopg
import pytest


from config import TEST_DB_CONN_STR


@pytest.fixture
def metadata_repo():
    from financial_data.metadata_repo import MetadataRepository

    return MetadataRepository(TEST_DB_CONN_STR)

@pytest.fixture(autouse=True)
def clean_metadata_tables():

    def _truncate_tables():
        with psycopg.connect(TEST_DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        metadata.pipeline_steps,
                        metadata.dataset_runs,
                        metadata.pipeline_runs
                    RESTART IDENTITY
                    """
                )

    _truncate_tables()
    yield
    _truncate_tables()