import argparse

import psycopg

from financial_data.runtime import RuntimeSettings


def check_connections() -> None:
    settings = RuntimeSettings.from_env()

    with (
        psycopg.connect(settings.database_conninfo()) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT current_user, current_database()")
        user, database = cursor.fetchone()

    print(f"POSTGRES_OK user={user} database={database}", flush=True)

    storage = settings.create_storage()

    # listing is lazy: request one result to perform the actual API call
    # an empty bucket is also a successful result
    next(
        iter(storage.list_objects(bucket="raw", prefix="")),
        None,
    )

    print("MINIO_OK bucket=raw", flush=True)
    print("CONNECTION_CHECK_PASSED", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Financial Data Platform ETL commands"
    )
    parser.add_argument(
        "command",
        choices=["check-connections"],
    )
    args = parser.parse_args()

    if args.command == "check-connections":
        check_connections()


if __name__ == "__main__":
    main()