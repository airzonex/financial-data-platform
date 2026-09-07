from typing import Protocol
from datetime import date

import psycopg

from financial_data.metadata_repo import MetadataRepository


class DatasetWatermarkProvider(Protocol):

    def get_max_date(self) -> date | None:
        ...


class RgbiWatermarkProvider:

    def __init__(self, connection_str: str, run_id: int) -> None:
        self.connection_str = connection_str
        self.run_id = run_id

    def get_max_date(self) -> date | None:
        query = """
            SELECT MAX(trade_date)
              FROM int.rgbi_history
             WHERE source_run_id = %s
        """

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.run_id,))
                row = cur.fetchone()

                return row[0] if row else None

class KeyrateWatermarkProvider:

    def __init__(self, connection_str: str, run_id: int) -> None:
        self.connection_str = connection_str
        self.run_id = run_id

    def get_max_date(self) -> date | None:
        query = """
            SELECT MAX(trade_date)
              FROM int.keyrate_history
             WHERE source_run_id = %s
        """

        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.run_id,))
                row = cur.fetchone()

                return row[0] if row else None

class DatasetRunFinalizer:

    def __init__(
            self,
            repository: MetadataRepository,
            providers: dict[str, DatasetWatermarkProvider]
    ) -> None:
        """
        В providers приходит словарь вида {"rgbi": RgbiWatermarkProvider, "keyrate": KeyrateWatermarkProvider}
        """

        self.repository = repository
        self.providers = providers

    def finalize(self, dataset_runs: dict[str, int]) -> None:
        """
        В dataset_runs приходит словарь вида {"rgbi": rgbi_run_id, "keyrate": keyrate_run_id}
        """

        for dataset, provider in self.providers.items():
            
            if dataset not in dataset_runs:
                raise ValueError(
                    f'Dataset run id is missing for dataset {dataset}'
                )

            run_id = dataset_runs[dataset]

            actual_max_date = provider.get_max_date()
            if actual_max_date is None:
                raise ValueError(
                    f'No data found for {dataset} dataset, run_id = {run_id}'
                )

            self.repository.finish_dataset_run(run_id, actual_max_date)