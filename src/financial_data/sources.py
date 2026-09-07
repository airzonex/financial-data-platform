from datetime import date, time
from dataclasses import dataclass

import httpx


CBR_DATE_FORMAT = '%d.%m.%Y'

class CbrClient:

    def __init__(self, base_url: str, date_start: date) -> None:
        self.base_url = base_url
        self.date_start = date_start

    def fetch_cbr_page(self) -> bytes:
        params = {
            'UniDbQuery.Posted': 'True',
            'UniDbQuery.From': self.date_start.strftime(CBR_DATE_FORMAT)
        }

        res = httpx.get(self.base_url, 
                        params=params, 
                        follow_redirects=True, 
                        timeout=10)
        res.raise_for_status()

        return res.content


@dataclass
class MoexPage:
    raw: bytes
    data: dict

class MoexClient:

    def __init__(
        self,
        base_url: str,
        date_start: date,
        http_client: httpx.Client
    ) -> None:
        self.base_url = base_url
        self.date_start = date_start
        self.http_client = http_client

    def fetch_rgbi_page(
        self,
        start: int = 0,
        limit: int | None = None,
    ) -> MoexPage:
        params = {
            "from": self.date_start.isoformat(),
            "start": start,
        }

        if limit is not None:
            params["limit"] = limit

        for attempt in range(1, 4):
            try:
                response = self.http_client.get(
                    self.base_url,
                    params=params
                )

                response.raise_for_status()

                return MoexPage(raw=response.content, data=response.json())

            except (
                httpx.RemoteProtocolError,
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.ConnectTimeout
            ):
                if attempt == 3:
                    raise

                time.sleep(2 ** (attempt - 1))