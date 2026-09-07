from dataclasses import dataclass
from datetime import date

from financial_data.sources import CbrClient, MoexClient, MoexPage
from financial_data.storage import MinioStorage


@dataclass(frozen=True)
class IngestionMetrics:
    run_id: int
    bucket: str
    objects_prefix: str
    objects_saved: int


class CbrKeyrateIngestion:

    def __init__(self,
                 client: CbrClient,
                 storage: MinioStorage,
                 bucket: str) -> None:
        self.client = client
        self.storage = storage
        self.bucket = bucket

    @staticmethod
    def _get_object_prefix(run_id: int,
                           run_date: date) -> str:
        return (
            'cbr/keyrate/'
            f'ingestion_date={run_date.isoformat()}/'
            f'run_id={run_id}'
        )

    @staticmethod
    def _get_object_name(object_prefix: str) -> str:
        return (
            f'{object_prefix}/part-0000.html'
        )

    def run(self, run_id: int, run_date: date) -> IngestionMetrics:
        # CbrClient возвращает одну страницу html
        object_prefix = self._get_object_prefix(run_id, run_date)
        object_name = self._get_object_name(object_prefix)

        page: bytes = self.client.fetch_cbr_page()

        self.storage.put(
            bucket=self.bucket,
            object_name=object_name,
            data=page,
            content_type='text/html'
        )

        return IngestionMetrics(run_id, self.bucket, object_prefix, 1)


class MoexRgbiIngestion:

    def __init__(self, 
                 client: MoexClient, 
                 storage: MinioStorage,
                 bucket: str) -> None:
        self.client = client
        self.storage = storage
        self.bucket = bucket

    @staticmethod
    def _get_objects_prefix(
        ingest_date: date,
        run_id: int
    ) -> str:
        return(
            'moex/rgbi/'
            f'ingestion_date={ingest_date.isoformat()}/'
            f'run_id={run_id}'
        )

    @staticmethod
    def _get_object_name(
            objects_prefix: str,
            part_number: int
    ) -> str:
        return(
            f'{objects_prefix}/'
            f'part-{part_number:04d}.json'
        )

    def run(self, run_id: int, run_date: date) -> IngestionMetrics:
        objects_prefix = self._get_objects_prefix(run_date, run_id)

        start = 0
        objects_saved = 0

        while True:
            page: MoexPage = self.client.fetch_rgbi_page(
                start=start,
            )

            # TODO: реализовать retry/backoff

            index, total, page_size = page.data["history.cursor"]["data"][0]

            if page_size <= 0:
                raise ValueError(
                    f'Invalid page size received from MOEX: {page_size}'
                )

            if total == 0:
                break

            part_number = index // page_size

            # сохранить raw в MinIO
            self.storage.put(
                bucket=self.bucket,
                object_name=self._get_object_name(
                    objects_prefix,
                    part_number
                ),
                data=page.raw,
                content_type='application/json'
            )

            objects_saved += 1
            next_start = index + page_size
            
            if next_start >= total:
                break

            start = next_start

        return IngestionMetrics(run_id, self.bucket, objects_prefix, objects_saved)