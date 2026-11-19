import pytest


@pytest.fixture(scope="session", autouse=True)
def prepare_e2e_database(ensure_test_database) -> None:
    pass