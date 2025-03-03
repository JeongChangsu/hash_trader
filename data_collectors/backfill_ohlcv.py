import asyncio
from data_collectors.market_data_collector import MarketDataCollector
import ccxt.async_support as ccxt
import logging


async def main():
    logging.basicConfig(level=logging.INFO)

    collector = MarketDataCollector(
        exchange_id="binance",
        symbols=["BTC/USDT"],
        timeframes=["1h", "4h", "1d", "1w"]
    )

    await collector.connect()

    # 원하는 만큼의 과거 데이터를 가져와서 저장
    timeframes_limits = {
        '1h': 500,  # 500시간 데이터 (~21일 분량)
        '4h': 500,  # ~83일 분량
        '1d': 200,  # 200일 데이터
        '1w': 50  # 약 1년 분량
    }

    for tf, limit in timeframes_limits.items():
        await collector.fetch_ohlcv_history("BTC/USDT", tf, limit=limit)

    await collector.exchange.close()
    await collector.redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
