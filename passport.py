import httpx
import datetime

from typing import Optional
from metrics import MARKETS, METRICS, EXTENSIONS, TradeStat, OrderStat, ObStat


class URLGenerator:
    BASE_URL = 'https://iss.moex.com/iss/datashop/algopack/'

    @staticmethod
    def generate_url(
            market: MARKETS,
            metric: METRICS,
            extension: EXTENSIONS,
            ticker: str = None,
            date: datetime = None,
            from_date: datetime = None,
            till_date: datetime = None
    ) -> str:
        url = f"{URLGenerator.BASE_URL}{market.value}/{metric.value}"

        if market is MARKETS.FUTURES and metric is METRICS.ORDER_STATS:
            raise Exception("Order stats is not defined for futures(fo) market")

        if ticker:
            url += f"/{ticker}{extension.value}"
            if from_date is not None and till_date is not None:
                url += f"?from={from_date.strftime('%Y-%m-%d')}&till={till_date.strftime('%Y-%m-%d')}"
            elif from_date is not None:
                url += f"?from={from_date.strftime('%Y-%m-%d')}"
            elif from_date is not None and till_date is None:
                raise Exception("error with from/till data")

        else:
            url += f"{extension.value}"
            if date is not None:
                url += f"?date={date.strftime('%Y-%m-%d')}"

        return url


class PassportClient(URLGenerator):
    AUTH_URL = 'https://passport.moex.com/authenticate'
    __AUTH_CERT = None

    def __init__(self, username: str, password: str):
        self.__username = username
        self.__password = password
        self.session = httpx.AsyncClient()

    async def authenticate(self):
        response = await self.session.get(self.AUTH_URL, auth=(self.__username, self.__password))
        if response.status_code == 200:
            self.__AUTH_CERT = response.cookies.get('MicexPassportCert')
            return self.__AUTH_CERT
        else:
            raise Exception("Authentication failed")

    async def make_authenticated_request(
            self,
            market: MARKETS,
            metric: METRICS,
            extension: EXTENSIONS,
            ticker: str = None,
            date: datetime.date = None,
            from_date: datetime.date = None,
            till_date: datetime.date = None
    ):
        url = super().generate_url(
            market=market,
            metric=metric,
            extension=extension,
            ticker=ticker,
            date=date,
            from_date=from_date,
            till_date=till_date
        )
        print(f"URL: {url}\n")

        headers = {
            'Cookie': f'MicexPassportCert={self.__AUTH_CERT}'
        }
        response = await self.session.get(url, headers=headers)

        if response.status_code == 200:
            if extension is (EXTENSIONS.JSON or EXTENSIONS.JSONP):

                data = response.json()

                if metric is METRICS.TRADE_STATS:
                    response = [TradeStat(
                        *[candle[_] for _ in range(21)]
                    ) for candle in data['data']['data']]

                elif metric is METRICS.ORDER_STATS:
                    response = [OrderStat(
                        *[candle[_] for _ in range(24)]
                    ) for candle in data['data']['data']]

                elif metric is METRICS.ORDERBOOK_STATS:
                    response = [ObStat(
                        *[candle[_] for _ in range(19)]
                    ) for candle in data['data']['data']]

        return response

