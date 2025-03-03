# market_data_collector.py
"""
시장 데이터 수집기

이 모듈은 ccxt.pro를 사용하여 다양한 거래소에서 실시간 시장 데이터를 수집합니다.
다중 시간프레임 OHLCV 데이터, 오더북, 거래량 프로필 등을 수집하고 처리합니다.
수집된 데이터는 Redis 및 PostgreSQL에 저장됩니다.
"""

import time
import json
import pytz
import logging
import asyncio
import psycopg2

import numpy as np
import pandas as pd
import ccxt.pro as ccxtpro
import redis.asyncio as redis

from datetime import datetime
from psycopg2.extras import Json
from typing import Dict, List, Optional, Tuple, Union, Any


class MarketDataCollector:
    """
    다양한 거래소에서 시장 데이터를 수집하고 저장하는 클래스
    """

    def __init__(
            self,
            exchange_id: str = "binance",
            symbols: List[str] = ["BTC/USDT"],
            timeframes: List[str] = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"],
            redis_host: str = "localhost",
            redis_port: int = 6379,
            redis_db: int = 0,
            pg_dbname: str = "magok_trader",
            pg_user: str = "hashmar",
            pg_password: str = "1111",
            pg_host: str = "localhost",
            pg_port: int = 5432,
            log_level: int = logging.INFO
    ):
        """
        MarketDataCollector 초기화

        Args:
            exchange_id: 사용할 거래소 ID (ccxt 지원 거래소)
            symbols: 수집할 심볼 목록
            timeframes: 수집할 시간프레임 목록
            redis_host: Redis 서버 호스트
            redis_port: Redis 서버 포트
            redis_db: Redis 데이터베이스 번호
            pg_dbname: PostgreSQL 데이터베이스 이름
            pg_user: PostgreSQL 사용자 이름
            pg_password: PostgreSQL 비밀번호
            pg_host: PostgreSQL 호스트
            pg_port: PostgreSQL 포트
            log_level: 로깅 레벨
        """
        # 로깅 설정
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            file_handler = logging.FileHandler("../../market_data.log")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # 설정 저장
        self.exchange_id = exchange_id
        self.symbols = symbols
        self.timeframes = timeframes

        # 거래소 초기화
        exchange_class = getattr(ccxtpro, exchange_id)
        self.exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 선물 거래에 초점
            }
        })

        # Redis 연결
        self.redis_params = {
            'host': redis_host,
            'port': redis_port,
            'db': redis_db
        }

        # PostgreSQL 연결 정보
        self.pg_params = {
            'dbname': pg_dbname,
            'user': pg_user,
            'password': pg_password,
            'host': pg_host,
            'port': pg_port
        }

        self.logger.info(
            f"MarketDataCollector initialized for {exchange_id} "
            f"with symbols {symbols} and timeframes {timeframes}"
        )

    async def initialize_db_tables(self) -> None:
        """
        필요한 PostgreSQL 테이블을 초기화합니다.
        """
        conn = psycopg2.connect(**self.pg_params)
        cursor = conn.cursor()

        try:
            # OHLCV 데이터 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_ohlcv (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    open NUMERIC NOT NULL,
                    high NUMERIC NOT NULL,
                    low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timeframe, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_market_ohlcv_lookup 
                ON market_ohlcv (exchange, symbol, timeframe, timestamp_ms);
            """)

            # 오더북 데이터 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_orderbook (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    bids JSONB NOT NULL,
                    asks JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_market_orderbook_lookup 
                ON market_orderbook (exchange, symbol, timestamp_ms);
            """)

            # 거래량 프로필 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS volume_profile (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    start_time_ms BIGINT NOT NULL,
                    end_time_ms BIGINT NOT NULL,
                    price_levels JSONB NOT NULL,
                    poc NUMERIC,
                    value_area_high NUMERIC,
                    value_area_low NUMERIC,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timeframe, start_time_ms, end_time_ms)
                );
            """)

            conn.commit()
            self.logger.info("Database tables initialized successfully")

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error initializing database tables: {e}")

        finally:
            cursor.close()
            conn.close()

    async def connect(self) -> None:
        """
        Redis에 연결하고 거래소 시장을 로드합니다.
        """
        try:
            self.redis_client = await redis.Redis(**self.redis_params)
            await self.redis_client.ping()
            self.logger.info("Successfully connected to Redis")

            # 거래소 시장 로드
            await self.exchange.load_markets()
            self.logger.info(f"Successfully loaded markets for {self.exchange_id}")

        except Exception as e:
            self.logger.error(f"Error connecting to services: {e}")
            raise

    async def fetch_multi_timeframe_ohlcv(
            self,
            symbol: str,
            limit: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        여러 시간프레임의 OHLCV 데이터를 수집합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            limit: 각 시간프레임당 가져올 캔들 수

        Returns:
            Dict[str, pd.DataFrame]: 시간프레임을 키로 하고 OHLCV 데이터프레임을 값으로 하는 딕셔너리
        """
        result = {}

        for timeframe in self.timeframes:
            try:
                # OHLCV 데이터 가져오기
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

                # pandas DataFrame으로 변환
                df = pd.DataFrame(
                    ohlcv,
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )

                # 로깅 시 한국 시간 표시
                latest_candle = df.iloc[-1]
                utc_time = datetime.fromtimestamp(latest_candle['timestamp'].timestamp())
                kst_time = utc_time.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Seoul'))

                self.logger.info(
                    f"{symbol} {timeframe} 캔들 {len(df)}개 수집 완료. "
                    f"최신 캔들 시간(KST): {kst_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )

                # 타임스탬프를 datetime으로 변환
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

                result[timeframe] = df

                # Redis에 저장
                for _, row in df.iterrows():
                    data = {
                        'timestamp_ms': int(row['timestamp'].timestamp() * 1000),
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': float(row['volume'])
                    }

                    redis_key = f"data:market:ohlcv:{self.exchange_id}:{symbol.replace('/', '_')}:{timeframe}:{data['timestamp_ms']}"
                    await self.redis_client.set(redis_key, json.dumps(data))

                # 최신 캔들 Redis 스트림에 저장
                latest_candle = df.iloc[-1].to_dict()
                latest_candle['timestamp_ms'] = int(latest_candle['timestamp'].timestamp() * 1000)
                latest_candle.pop('timestamp')

                stream_key = f"stream:market:ohlcv:{self.exchange_id}:{symbol.replace('/', '_')}:{timeframe}"
                await self.redis_client.xadd(
                    stream_key,
                    {'data': json.dumps(latest_candle)},
                    maxlen=1000,
                    approximate=True
                )

                # PostgreSQL에 저장
                await self.save_ohlcv_to_postgres(symbol, timeframe, df)

                self.logger.info(f"Fetched and stored {len(df)} {timeframe} candles for {symbol}")

            except Exception as e:
                self.logger.error(f"Error fetching {timeframe} OHLCV for {symbol}: {e}")

        return result

    async def fetch_ohlcv_history(self, symbol: str, timeframe: str, since: Optional[int] = None, limit: int = 1000):
        """
        과거 OHLCV 데이터를 수집하여 DB에 저장
        """
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # DB에 저장
            await self.save_ohlcv_to_postgres(symbol, timeframe, df)

            self.logger.info(f"과거 OHLCV 데이터를 저장함 ({symbol}, {timeframe}, {len(df)}개)")
        except Exception as e:
            self.logger.error(f"OHLCV 과거 데이터 저장 중 오류 발생: {e}")

    async def save_ohlcv_to_postgres(
            self,
            symbol: str,
            timeframe: str,
            df: pd.DataFrame
    ) -> None:
        """
        OHLCV 데이터를 PostgreSQL에 저장합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframe: 시간프레임(예: '1h')
            df: OHLCV 데이터프레임
        """
        conn = psycopg2.connect(**self.pg_params)
        cursor = conn.cursor()

        try:
            for _, row in df.iterrows():
                timestamp_ms = int(row['timestamp'].timestamp() * 1000)

                cursor.execute("""
                    INSERT INTO market_ohlcv 
                    (exchange, symbol, timeframe, timestamp_ms, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, symbol, timeframe, timestamp_ms) 
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    self.exchange_id,
                    symbol,
                    timeframe,
                    timestamp_ms,
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['volume'])
                ))

            conn.commit()

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving OHLCV data to PostgreSQL: {e}")

        finally:
            cursor.close()
            conn.close()

    async def fetch_order_book(self, symbol: str, limit: int = 100) -> Dict:
        """
        오더북 데이터를 수집합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            limit: 오더북 깊이

        Returns:
            Dict: 오더북 데이터
        """
        try:
            # 오더북 데이터 가져오기
            orderbook = await self.exchange.fetch_order_book(symbol, limit)
            timestamp_ms = int(time.time() * 1000)

            # 결과 구성
            result = {
                'timestamp_ms': timestamp_ms,
                'bids': orderbook['bids'],
                'asks': orderbook['asks']
            }

            # Redis에 저장
            redis_key = f"data:market:orderbook:{self.exchange_id}:{symbol.replace('/', '_')}"
            await self.redis_client.set(redis_key, json.dumps(result))

            # Redis 스트림에 저장
            stream_key = f"stream:market:orderbook:{self.exchange_id}:{symbol.replace('/', '_')}"
            await self.redis_client.xadd(
                stream_key,
                {'data': json.dumps(result)},
                maxlen=1000,
                approximate=True
            )

            # PostgreSQL에 저장
            conn = psycopg2.connect(**self.pg_params)
            cursor = conn.cursor()

            try:
                cursor.execute("""
                    INSERT INTO market_orderbook 
                    (exchange, symbol, timestamp_ms, bids, asks)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, symbol, timestamp_ms) 
                    DO UPDATE SET
                        bids = EXCLUDED.bids,
                        asks = EXCLUDED.asks,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    self.exchange_id,
                    symbol,
                    timestamp_ms,
                    Json(result['bids']),
                    Json(result['asks'])
                ))

                conn.commit()

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error saving orderbook data to PostgreSQL: {e}")

            finally:
                cursor.close()
                conn.close()

            self.logger.info(
                f"Fetched and stored orderbook for {symbol} with {len(orderbook['bids'])} bids and {len(orderbook['asks'])} asks")

            return result

        except Exception as e:
            self.logger.error(f"Error fetching orderbook for {symbol}: {e}")
            return None

    async def calculate_volume_profile(
            self,
            symbol: str,
            timeframe: str = '1d',
            periods: int = 30,
            num_bins: int = 100,
            value_area_pct: float = 0.70
    ) -> Dict:
        """
        거래량 프로필을 계산합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframe: 시간프레임(예: '1d')
            periods: 분석할 기간 수
            num_bins: 가격 수준 수
            value_area_pct: 거래량 가치 영역 비율(0-1)

        Returns:
            Dict: 거래량 프로필 데이터
        """
        try:
            # OHLCV 데이터 가져오기
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=periods)

            # pandas DataFrame으로 변환
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

            # 가격 범위 결정
            price_low = df['low'].min()
            price_high = df['high'].max()

            # 가격 구간 생성
            price_bins = np.linspace(price_low, price_high, num_bins)
            price_range = (price_high - price_low) / (num_bins - 1)

            # 각 가격 수준에서의 거래량 계산
            volume_profile = {float(price): 0.0 for price in price_bins}

            for _, row in df.iterrows():
                candle_range = row['high'] - row['low']
                if candle_range <= 0:
                    continue

                # 캔들의 각 가격 구간에 거래량 분배
                for price in price_bins:
                    if price >= row['low'] and price <= row['high']:
                        # 간단한 가중치 적용: 종가에 가까울수록 거래량이 더 많이 분배됨
                        weight = 1 - abs(price - row['close']) / candle_range
                        volume_profile[float(price)] += row['volume'] * weight / num_bins

            # 결과 정리
            price_levels = [
                {'price': price, 'volume': volume}
                for price, volume in volume_profile.items()
            ]

            # POC(Point of Control) 계산: 최대 거래량의 가격
            max_volume_price = max(volume_profile.items(), key=lambda x: x[1])
            poc = max_volume_price[0]

            # 가치 영역(Value Area) 계산
            sorted_levels = sorted(
                price_levels,
                key=lambda x: x['volume'],
                reverse=True
            )

            total_volume = sum(level['volume'] for level in price_levels)
            target_volume = total_volume * value_area_pct

            cumulative_volume = 0
            value_area_prices = []

            for level in sorted_levels:
                cumulative_volume += level['volume']
                value_area_prices.append(level['price'])
                if cumulative_volume >= target_volume:
                    break

            # 가치 영역의 최고가와 최저가
            value_area_high = max(value_area_prices)
            value_area_low = min(value_area_prices)

            # 결과 구성
            result = {
                'symbol': symbol,
                'timeframe': timeframe,
                'start_time_ms': int(df['timestamp'].iloc[0]),
                'end_time_ms': int(df['timestamp'].iloc[-1]),
                'price_levels': price_levels,
                'poc': poc,
                'value_area_high': value_area_high,
                'value_area_low': value_area_low
            }

            # Redis에 저장
            redis_key = f"data:market:volume_profile:{self.exchange_id}:{symbol.replace('/', '_')}:{timeframe}"
            await self.redis_client.set(redis_key, json.dumps(result))

            # PostgreSQL에 저장
            conn = psycopg2.connect(**self.pg_params)
            cursor = conn.cursor()

            try:
                cursor.execute("""
                    INSERT INTO volume_profile 
                    (exchange, symbol, timeframe, start_time_ms, end_time_ms, 
                     price_levels, poc, value_area_high, value_area_low)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, symbol, timeframe, start_time_ms, end_time_ms) 
                    DO UPDATE SET
                        price_levels = EXCLUDED.price_levels,
                        poc = EXCLUDED.poc,
                        value_area_high = EXCLUDED.value_area_high,
                        value_area_low = EXCLUDED.value_area_low,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    self.exchange_id,
                    symbol,
                    timeframe,
                    result['start_time_ms'],
                    result['end_time_ms'],
                    Json(result['price_levels']),
                    result['poc'],
                    result['value_area_high'],
                    result['value_area_low']
                ))

                conn.commit()

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error saving volume profile to PostgreSQL: {e}")

            finally:
                cursor.close()
                conn.close()

            self.logger.info(f"Calculated and stored volume profile for {symbol} ({timeframe})")

            return result

        except Exception as e:
            self.logger.error(f"Error calculating volume profile for {symbol}: {e}")
            return None

    async def run_collection_cycle(self) -> None:
        """
        모든 심볼에 대한 데이터 수집 사이클을 실행합니다.
        """
        for symbol in self.symbols:
            try:
                # OHLCV 데이터 수집
                await self.fetch_multi_timeframe_ohlcv(symbol)

                # 오더북 데이터 수집
                await self.fetch_order_book(symbol)

                # 거래량 프로필 계산 (1일 및 4시간 시간프레임)
                await self.calculate_volume_profile(symbol, '1d', 30)
                await self.calculate_volume_profile(symbol, '4h', 120)

            except Exception as e:
                self.logger.error(f"Error in collection cycle for {symbol}: {e}")

    async def run(self, interval_seconds: int = 60) -> None:
        """
        데이터 수집기를 실행합니다.

        Args:
            interval_seconds: 수집 사이클 간격(초)
        """
        try:
            await self.connect()
            await self.initialize_db_tables()

            self.logger.info(
                f"Starting market data collection with {interval_seconds}s interval"
            )

            while True:
                start_time = time.time()

                await self.run_collection_cycle()

                # 다음 사이클까지 대기
                elapsed = time.time() - start_time
                wait_time = max(0, interval_seconds - elapsed)

                if wait_time > 0:
                    self.logger.info(
                        f"Collection cycle completed in {elapsed:.2f}s. "
                        f"Waiting {wait_time:.2f}s for next cycle."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.warning(
                        f"Collection cycle took {elapsed:.2f}s, "
                        f"which is longer than the interval of {interval_seconds}s"
                    )

        except Exception as e:
            self.logger.error(f"Error in market data collector: {e}")

        finally:
            if hasattr(self, 'redis_client'):
                await self.redis_client.close()

            if hasattr(self, 'exchange'):
                await self.exchange.close()

            self.logger.info("Market data collector stopped")


# 직접 실행 시 데이터 수집기 시작
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 데이터 수집기 초기화
    collector = MarketDataCollector(
        exchange_id="binance",
        symbols=["BTC/USDT"],
        timeframes=["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
    )

    # 이벤트 루프 얻기
    loop = asyncio.get_event_loop()

    try:
        # 데이터 수집기 실행
        loop.run_until_complete(collector.run())
    except KeyboardInterrupt:
        logging.info("Data collection interrupted by user")
    finally:
        # 이벤트 루프 종료
        loop.close()
