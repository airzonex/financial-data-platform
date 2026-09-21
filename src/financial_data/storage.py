from collections.abc import Iterator
from io import BytesIO

from minio import Minio


class MinioStorage:

    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            secure: bool = False,
    ) -> None:
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def put(
            self,
            bucket: str,
            object_name: str,
            data: bytes,
            content_type: str,
    ) -> None:
        self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def list_objects(
        self,
        bucket: str,
        prefix: str
    ) -> Iterator[str]:
        for obj in self.client.list_objects(
            bucket,
            prefix,
            recursive=True
        ):
            yield obj.object_name

    def get(
        self,
        bucket: str,
        object_name: str
    ) -> bytes:
        response = self.client.get_object(
            bucket_name=bucket,
            object_name=object_name
        )

        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()