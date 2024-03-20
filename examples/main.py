import asyncio
from metrics import *
from passport import PassportClient
import datetime


async def test():
    pc = PassportClient(username="", password="")
    await pc.authenticate()
    res = await pc.make_authenticated_request(
        market=MARKETS.STOCKS,
        metric=METRICS.TRADE_STATS,
        extension=EXTENSIONS.JSON,
        ticker='ASTR',
        date=datetime.date(2024, 3, 19),
        from_date=datetime.date(2024, 3, 18),
        till_date=datetime.date(2024, 3, 19)
    )

    print(res)


asyncio.run(test())


