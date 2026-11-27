from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

# application-defined identifier, shared by all financial dbt builds
DBT_BUILD_LOCK_KEY = 10000001


class DbtBuildBusyError(RuntimeError):
    pass


@contextmanager
def dbt_build_lock(connection_str: str) -> Iterator[None]:
    with psycopg.connect(
        connection_str,
        autocommit=True,
    ) as conn:
        row = conn.execute(
            """
            SELECT pg_try_advisory_lock(
                %s::bigint
            )
            """,
            (DBT_BUILD_LOCK_KEY,),
        ).fetchone()

        if row is None or not row[0]:
            raise DbtBuildBusyError(
                "Another financial dbt build holds the lock"
            )

        yield

    # exiting the connection context closes this dedicated session
    # releasing its session-level advisory lock