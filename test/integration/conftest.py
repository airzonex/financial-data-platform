import pytest


from config import TEST_DB_CONN_STR


@pytest.fixture
def metadata_repo():
    from financial_data.metadata_repo import MetadataRepository

    return MetadataRepository(TEST_DB_CONN_STR)