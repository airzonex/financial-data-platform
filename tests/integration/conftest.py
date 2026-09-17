import uuid
from typing import Iterator

import pytest
from minio import Minio

from config import TEST_DB_CONN_STR, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from financial_data.storage import MinioStorage


@pytest.fixture
def metadata_repo():
    from financial_data.metadata_repo import MetadataRepository

    return MetadataRepository(TEST_DB_CONN_STR)


@pytest.fixture
def minio_storage() -> MinioStorage:
    return MinioStorage(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

@pytest.fixture
def test_prefix() -> str:
    return f'test/{uuid.uuid4()}'

@pytest.fixture(scope='session')
def minio_client() -> Minio:
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client

@pytest.fixture(autouse=True)
def cleanup_test_objects(
    minio_client: Minio,
    #test_prefix: str
) -> Iterator[None]:
    yield

    for object in minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        #prefix=test_prefix,
        recursive=True
    ):
        minio_client.remove_object(
            bucket_name=MINIO_BUCKET,
            object_name=object.object_name
        )