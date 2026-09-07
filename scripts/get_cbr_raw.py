from datetime import date

from financial_data.sources.cbr_client import CbrClient


CBR_URL = 'https://www.cbr.ru/hd_base/KeyRate'

date_start = date(2014, 1, 1)

cbr_client = CbrClient(CBR_URL, date_start)
print(cbr_client.fetch_cbr_page())