import psycopg
import json
from dataclasses import dataclass

from bs4 import BeautifulSoup

from financial_data.storage import MinioStorage


@dataclass
class StagingMetrics:
    run_id: int
    records_loaded: int


class KeyrateStagingLoader:

    def __init__(self, run_id: int, storage: MinioStorage, bucket: str, prefix: str, db_conn_str: str):
        self.run_id = run_id
        self.storage = storage
        self.bucket = bucket
        self.prefix = prefix
        self.db_conn_str = db_conn_str

    def _get_single_object_name(self) -> str:
        objects = self.storage.list_objects(
            bucket=self.bucket,
            prefix=self.prefix
        )

        try:
            object_name = next(objects)
        except StopIteration:
            raise FileNotFoundError(
                f'No objects found under {self.bucket}/{self.prefix}'
            )

        try:
            next(objects)
        except StopIteration:
            return object_name

        raise ValueError(
            f'Expected exactly one object under {self.bucket}/{self.prefix}'
        )

    def _parse_raw_bytes(self, raw_bytes: bytes) -> tuple[tuple[int, str, str], ...]:
        html = raw_bytes.decode(encoding='utf-8')
        
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        if not table:
            raise ValueError(
                'Tag table not found in soup'
            )

        rows = table.find_all('tr')[1:]

        data: tuple[tuple[int, str, str], ...] = []
        for row in rows:
            cols = [c.text.strip() for c in row.find_all('td')]
            if len(cols) >= 2:
                data.append((self.run_id, cols[0], cols[1]))

        if not data:
            raise ValueError(
                'Table is empty or has an invalid format'
            )

        return tuple(data)


    def _clear_history(self, cur) -> None:
        query = """
            DELETE FROM stg.keyrate_history
             WHERE run_id = %s
        """

        cur.execute(query, (self.run_id,))


    def _get_metrics(self, cur) -> StagingMetrics:
        query = """
            SELECT COUNT(*)
              FROM stg.keyrate_history
             WHERE run_id = %s
        """

        cur.execute(query, (self.run_id,))
        records_loaded = cur.fetchone()[0]

        return StagingMetrics(
            run_id=self.run_id,
            records_loaded=records_loaded
        )


    def load(self) -> StagingMetrics:

        insert_query = """
            INSERT INTO stg.keyrate_history(
                run_id,
                trade_date,
                keyrate)
            VALUES (%s, %s, %s)
        """

        object_name = self._get_single_object_name()

        raw_bytes = self.storage.get(
            bucket=self.bucket,
            object_name=object_name
        )

        data = self._parse_raw_bytes(raw_bytes)

        with psycopg.connect(self.db_conn_str) as conn:
            with conn.cursor() as cur:
                self._clear_history(cur)
                cur.executemany(insert_query, data)
                metrics: StagingMetrics = self._get_metrics(cur)

        return metrics


FLD_TRADE_DATE = 'TRADEDATE'
FLD_CLOSE = 'CLOSE'
FLD_CURRENCY = 'CURRENCYID'

class RgbiStagingLoader:

    def __init__(self, run_id: int, storage: MinioStorage, bucket: str, prefix: str, db_conn_str: str) -> None:
        self.run_id = run_id
        self.storage = storage
        self.bucket = bucket
        self.prefix = prefix
        self.db_conn_str = db_conn_str

    def _clear_history(self, cur) -> None:
        query = """
            DELETE FROM stg.rgbi_history
             WHERE run_id = %s
        """

        cur.execute(query, (self.run_id,))

    def _get_metrics(self, cur) -> StagingMetrics:
        query = """
            SELECT COUNT(*)
              FROM stg.rgbi_history
             WHERE run_id = %s
        """

        cur.execute(query, (self.run_id,))
        records_loaded = cur.fetchone()[0]

        return StagingMetrics(
            run_id=self.run_id,
            records_loaded=records_loaded
        )

    def load(self) -> StagingMetrics:
        insert_query = """
            INSERT INTO stg.rgbi_history (
                run_id, 
                trade_date, 
                close, 
                currency_id)
            VALUES (%s, %s, %s, %s)
        """

        with psycopg.connect(self.db_conn_str) as conn:
            with conn.cursor() as cur:

                self._clear_history(cur)

                for object_name in self.storage.list_objects(
                    bucket=self.bucket,
                    prefix=self.prefix
                ):
                    if not object_name.endswith('.json'):
                        continue

                    raw_bytes = self.storage.get(
                        bucket=self.bucket,
                        object_name=object_name
                    )

                    page = json.loads(raw_bytes)

                    columns = page['history']['columns']
                    data = page['history']['data']

                    date_idx = columns.index(FLD_TRADE_DATE)
                    close_idx = columns.index(FLD_CLOSE)
                    currency_idx = columns.index(FLD_CURRENCY)

                    rows = [
                        (
                            self.run_id,
                            row[date_idx],
                            row[close_idx],
                            row[currency_idx]
                        )
                        for row in data
                    ] 

                    cur.executemany(insert_query, rows)

                metrics: StagingMetrics = self._get_metrics(cur)

        return metrics