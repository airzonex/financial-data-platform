from config import MINIO_BUCKET

from financial_data.storage import MinioStorage


def test_put_and_get_objects(
    minio_storage: MinioStorage,
    test_prefix
) -> None:
    object_name = f'{test_prefix}/part-0000.txt'
    data = b'test data'
    
    minio_storage.put(
        bucket=MINIO_BUCKET,
        object_name=object_name,
        data=data,
        content_type='text/plain'
    )

    res = minio_storage.get(
        bucket=MINIO_BUCKET,
        object_name=object_name
    )

    assert res == data

def test_list_objects_returns_objects_under_prefix(
    minio_storage: MinioStorage,
    test_prefix: str
) -> None:
    objects = {
        f'{test_prefix}/part-0000.json': b'first',
        f'{test_prefix}/part-0001.json': b'second',
        'some/other/prefix/part-0000.json': b'other'
    }

    for object_name, data in objects.items():
        minio_storage.put(
            bucket=MINIO_BUCKET,
            object_name=object_name,
            data=data,
            content_type='application/json'
        )

    res = list(minio_storage.list_objects(
        bucket=MINIO_BUCKET,
        prefix=test_prefix
    ))

    assert sorted(res) == sorted([
        f'{test_prefix}/part-0000.json',
        f'{test_prefix}/part-0001.json'
    ])

def test_put_overrides_existing_object(
    minio_storage: MinioStorage,
    test_prefix: str
) -> None:
    object_name = f'{test_prefix}/part-0000.json'

    minio_storage.put(
        bucket=MINIO_BUCKET,
        object_name=object_name,
        data=b'first',
        content_type='application/json'
    )

    minio_storage.put(
        bucket=MINIO_BUCKET,
        object_name=object_name,
        data=b'second',
        content_type='application/json'
    )

    res = minio_storage.get(
        bucket=MINIO_BUCKET,
        object_name=object_name
    )

    assert res == b'second'