# data_collectors/backfill_ohlcv.py
"""
OHLCV 과거 데이터 백필 모듈

이 모듈은 지정된 심볼과 시간프레임에 대한 과거 OHLCV 데이터를 수집하여
데이터베이스에 저장합니다. 커맨드라인에서 직접 실행하거나 다른 모듈에서
임포트하여 사용할 수 있습니다.
"""

import asyncio
import argparse
import logging
from typing import List, Dict, Optional, Union
import sys
import os

# 프로젝트 루트 디렉토리 추가 (독립 실행 시 필요)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collectors.market_data_collector import MarketDataCollector
from config.settings import TRADING_SYMBOLS, TIMEFRAMES
from config.logging_config import configure_logging


async def backfill_ohlcv(
        exchange_id: str = "binance",
        symbols: List[str] = None,
        timeframes: List[str] = None,
        days: Dict[str, int] = None,
        custom_limits: Dict[str, int] = None
) -> None:
    """
    지정된 심볼과 시간프레임에 대한 과거 OHLCV 데이터를 수집합니다.

    Args:
        exchange_id: 거래소 ID (기본값: "binance")
        symbols: 수집할 심볼 목록 (기본값: settings.py의 TRADING_SYMBOLS)
        timeframes: 수집할 시간프레임 목록 (기본값: settings.py의 TIMEFRAMES)
        days: 각 시간프레임별 수집할 일수 (기본값: None, 자동 계산)
        custom_limits: 각 시간프레임별 수집할 캔들 수 (기본값: None, days 기반 계산)
    """
    # 기본값 설정
    symbols = symbols or TRADING_SYMBOLS
    timeframes = timeframes or TIMEFRAMES

    # 로거 설정
    logger = configure_logging("backfill_ohlcv")

    logger.info(f"OHLCV 데이터 백필 시작: {symbols} - {timeframes}")

    # MarketDataCollector 인스턴스 생성
    collector = MarketDataCollector(
        exchange_id=exchange_id,
        symbols=symbols,
        timeframes=timeframes
    )

    # 연결 및 DB 초기화
    try:
        await collector.connect()
        await collector.initialize_db_tables()

        # 시간프레임별 기본 한도 계산
        # 1일(24시간)을 기준으로 각 시간프레임별 캔들 수 계산
        default_timeframe_limits = calculate_limits(timeframes, days)

        # 사용자 정의 한도가 있으면 우선 적용
        timeframe_limits = custom_limits or default_timeframe_limits

        # 각 심볼과 시간프레임별로 데이터 수집
        for symbol in symbols:
            for tf in timeframes:
                limit = timeframe_limits.get(tf, 100)  # 기본값 100

                logger.info(f"{symbol} {tf} 시간프레임 데이터 {limit}개 수집 중...")
                await collector.fetch_ohlcv_history(symbol, tf, limit=limit)

        logger.info("OHLCV 데이터 백필 완료")

    except Exception as e:
        logger.error(f"백필 중 오류 발생: {e}")
    finally:
        # 리소스 정리
        try:
            if hasattr(collector, 'exchange'):
                await collector.exchange.close()
            if hasattr(collector, 'redis_client'):
                await collector.redis_client.close()
            logger.info("리소스 정리 완료")
        except Exception as e:
            logger.error(f"리소스 정리 중 오류 발생: {e}")


def calculate_limits(timeframes: List[str], days: Dict[str, int] = None) -> Dict[str, int]:
    """
    각 시간프레임별로 캔들 수를 계산합니다.

    Args:
        timeframes: 시간프레임 목록
        days: 각 시간프레임별 수집할 일수 (지정되지 않으면 기본값 사용)

    Returns:
        Dict[str, int]: 시간프레임별 캔들 수
    """
    # 시간프레임별 기본 일수 (지정되지 않은 경우)
    default_days = {
        "1m": 7,  # 1분봉은 7일치 (약 10,080개)
        "5m": 30,  # 5분봉은 30일치 (약 8,640개)
        "15m": 60,  # 15분봉은 60일치 (약 5,760개)
        "30m": 90,  # 30분봉은 90일치 (약 4,320개)
        "1h": 180,  # 1시간봉은 180일치 (약 4,320개)
        "2h": 180,  # 2시간봉은 180일치 (약 2,160개)
        "4h": 365,  # 4시간봉은 365일치 (약 2,190개)
        "6h": 365,  # 6시간봉은 365일치 (약 1,460개)
        "12h": 365,  # 12시간봉은 365일치 (약 730개)
        "1d": 730,  # 일봉은 2년치 (약 730개)
        "3d": 1095,  # 3일봉은 3년치 (약 365개)
        "1w": 520,  # 주봉은 10년치 (약 520개)
        "1M": 120  # 월봉은 10년치 (약 120개)
    }

    # 사용자 지정 일수가 있으면 적용
    if days:
        for tf, day_count in days.items():
            default_days[tf] = day_count

    # 일수를 캔들 수로 변환
    timeframe_to_minutes = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "6h": 360,
        "12h": 720,
        "1d": 1440,
        "3d": 4320,
        "1w": 10080,
        "1M": 43200  # 30일 기준
    }

    limits = {}
    for tf in timeframes:
        if tf in default_days and tf in timeframe_to_minutes:
            # 일수 * 하루 분 수 / 시간프레임 분 수 = 캔들 수
            candles = default_days[tf] * 24 * 60 // timeframe_to_minutes[tf]

            # 거래소 API 제한 고려 (일반적으로 1000개 제한이 있음)
            limits[tf] = min(candles, 1000)

    return limits


async def main():
    """
    커맨드라인에서 실행 시 시작점
    """
    parser = argparse.ArgumentParser(description="OHLCV 과거 데이터 백필 도구")

    parser.add_argument("--exchange", type=str, default="binance",
                        help="사용할 거래소 (기본값: binance)")
    parser.add_argument("--symbols", type=str, nargs="+",
                        help="수집할 심볼 목록 (기본값: settings.py의 TRADING_SYMBOLS)")
    parser.add_argument("--timeframes", type=str, nargs="+",
                        help="수집할 시간프레임 목록 (기본값: settings.py의 TIMEFRAMES)")

    # 시간프레임별 일수를 지정하는 인자 추가
    parser.add_argument("--days-1m", type=int, help="1분봉 수집 일수")
    parser.add_argument("--days-5m", type=int, help="5분봉 수집 일수")
    parser.add_argument("--days-15m", type=int, help="15분봉 수집 일수")
    parser.add_argument("--days-1h", type=int, help="1시간봉 수집 일수")
    parser.add_argument("--days-4h", type=int, help="4시간봉 수집 일수")
    parser.add_argument("--days-1d", type=int, help="일봉 수집 일수")
    parser.add_argument("--days-1w", type=int, help="주봉 수집 일수")

    # 모든 시간프레임에 동일한 일수 적용
    parser.add_argument("--days-all", type=int, help="모든 시간프레임 수집 일수")

    args = parser.parse_args()

    # 시간프레임별 수집 일수 설정
    days = {}

    # 개별 시간프레임 일수 설정
    if args.days_1m:
        days["1m"] = args.days_1m
    if args.days_5m:
        days["5m"] = args.days_5m
    if args.days_15m:
        days["15m"] = args.days_15m
    if args.days_1h:
        days["1h"] = args.days_1h
    if args.days_4h:
        days["4h"] = args.days_4h
    if args.days_1d:
        days["1d"] = args.days_1d
    if args.days_1w:
        days["1w"] = args.days_1w

    # 모든 시간프레임에 동일한 일수 적용
    if args.days_all:
        timeframes = args.timeframes or TIMEFRAMES
        days = {tf: args.days_all for tf in timeframes}

    # 백필 실행
    await backfill_ohlcv(
        exchange_id=args.exchange,
        symbols=args.symbols,
        timeframes=args.timeframes,
        days=days
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
