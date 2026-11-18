import os
from dataclasses import dataclass, field

from psycopg.conninfo import make_conninfo

from financial_data.storage import MinioStorage


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class RuntimeSettings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str = field(repr=False)

    minio_endpoint: str
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)
    minio_secure: bool

    @classmethod
    def from_env(cls) -> "RuntimeSettings":
        try:
            db_port = int(required_env("DBT_PORT"))
        except ValueError:
            raise ValueError(
                "DBT_PORT must be an integer between 1 and 65535"
            ) from None

        if not 1 <= db_port <= 65535:
            raise ValueError("DBT_PORT must be between 1 and 65535")

        secure_value = required_env("MINIO_SECURE").strip().lower()
        if secure_value not in {"true", "false"}:
            raise ValueError("MINIO_SECURE must be true or false")

        endpoint = required_env("MINIO_ENDPOINT")
        if "://" in endpoint:
            raise ValueError(
                "MINIO_ENDPOINT must be host:port, without http:// or https://"
            )

        return cls(
            db_host=required_env("DBT_HOST"),
            db_port=db_port,
            db_name=required_env("DBT_DBNAME"),
            db_user=required_env("DBT_USER"),
            db_password=required_env("DBT_PASSWORD"),
            minio_endpoint=endpoint,
            minio_access_key=required_env("MINIO_ACCESS_KEY"),
            minio_secret_key=required_env("MINIO_SECRET_KEY"),
            minio_secure=secure_value == "true",
        )

    def database_conninfo(self) -> str:
        return make_conninfo(
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            connect_timeout=10,
        )

    def create_storage(self) -> MinioStorage:
        return MinioStorage(
            endpoint=self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure,
        )