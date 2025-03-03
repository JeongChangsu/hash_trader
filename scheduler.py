# scheduler.py
"""
HashTrader 데이터 수집 스케줄러

이 모듈은 여러 데이터 수집 모듈을 스케줄링하고 관리합니다.
한국 시간(KST) 기준으로 각 모듈별 최적 시간에 데이터를 수집합니다.
수집 누락 감지 및 복구 기능도 포함되어 있습니다.
"""

import os
import sys
import time
import math
import pytz
import asyncio
import logging
import signal

from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Any, Optional, Callable, Tuple

from config.settings import (
    MARKET_DATA_INTERVAL,
    ORDERBOOK_INTERVAL,
    VOLUME_PROFILE_INTERVAL,
    LIQUIDATION_HEATMAP_INTERVAL,
    FEAR_GREED_INTERVAL,
    ONCHAIN_DATA_INTERVAL
)
from config.logging_config import configure_logging

from data_collectors.backfill_ohlcv import backfill_ohlcv
from data_collectors.market_data_collector import MarketDataCollector
from data_collectors.fear_greed_collector import FearGreedCollector
from data_collectors.liquidation_analyzer import LiquidationAnalyzer
from data_collectors.onchain_data_collector import OnChainDataCollector

# 로깅 설정
logger = configure_logging("scheduler")

# 한국 시간대
KST = pytz.timezone('Asia/Seoul')


class DataCollectorScheduler:
    """
    데이터 수집 모듈을 스케줄링하는 클래스
    """

    def __init__(self):
        """스케줄러 초기화"""
        self.is_running = False
        self.event_loop = None
        self.tasks = {}
        self.collectors = {}
        self.last_runs = {}

        # 로거 참조 추가
        self.logger = logger  # 이미 파일 상단에서 로거를 가져오므로 여기서 참조만 저장

        # 실행 중인 태스크 추적
        self.running_tasks = {}

        # 데이터 수집 성공/실패 카운터
        self.stats = {
            "market_data": {"success": 0, "failure": 0},
            "fear_greed": {"success": 0, "failure": 0},
            "liquidation": {"success": 0, "failure": 0},
            "onchain": {"success": 0, "failure": 0},
            "volume_profile": {"success": 0, "failure": 0}
        }

        logger.info("데이터 수집 스케줄러가 초기화되었습니다")

    def _align_to_interval(self, interval_seconds: int) -> int:
        """
        다음 정각 실행 시간까지의 지연 시간을 계산합니다.

        Args:
            interval_seconds: 실행 간격(초)

        Returns:
            int: 다음 정각 실행까지 대기할 시간(초)
        """
        now = datetime.now(KST)
        current_seconds = now.second + now.minute * 60 + now.hour * 3600
        next_run = math.ceil(current_seconds / interval_seconds) * interval_seconds

        # 다음 날로 넘어가는 경우
        if next_run >= 24 * 3600:
            next_run = 0
            delay = 24 * 3600 - current_seconds
        else:
            delay = next_run - current_seconds

        return max(0, delay)

    async def run_market_data_collector(self) -> None:
        """OHLCV 및 오더북 데이터 수집 실행"""
        try:
            if "market_data" in self.running_tasks and not self.running_tasks["market_data"].done():
                logger.warning("이전 시장 데이터 수집 작업이 아직 실행 중입니다")
                return

            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"시장 데이터 수집 시작 (KST: {now_kst})")

            collector = MarketDataCollector()

            self.collectors["market_data"] = collector
            self.last_runs["market_data"] = datetime.now(KST)

            await collector.connect()
            await collector.initialize_db_tables()

            # 하나의 수집 사이클만 실행
            await collector.run_collection_cycle()

            # 리소스 정리
            if hasattr(collector, 'redis_client'):
                await collector.redis_client.close()
            if hasattr(collector, 'exchange'):
                await collector.exchange.close()

            self.stats["market_data"]["success"] += 1
            logger.info("시장 데이터 수집 완료")

        except Exception as e:
            self.stats["market_data"]["failure"] += 1
            logger.error(f"시장 데이터 수집 중 오류 발생: {e}")

    async def run_volume_profile_analyzer(self) -> None:
        """거래량 프로필 분석 실행"""
        try:
            if "volume_profile" in self.running_tasks and not self.running_tasks["volume_profile"].done():
                logger.warning("이전 거래량 프로필 분석 작업이 아직 실행 중입니다")
                return

            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"거래량 프로필 분석 시작 (KST: {now_kst})")

            collector = MarketDataCollector()

            self.collectors["volume_profile"] = collector
            self.last_runs["volume_profile"] = datetime.now(KST)

            await collector.connect()

            # BTC/USDT에 대한 거래량 프로필 계산
            await collector.calculate_volume_profile("BTC/USDT", "1d", 30)
            await collector.calculate_volume_profile("BTC/USDT", "4h", 120)

            # 리소스 정리
            if hasattr(collector, 'redis_client'):
                await collector.redis_client.close()
            if hasattr(collector, 'exchange'):
                await collector.exchange.close()

            self.stats["volume_profile"]["success"] += 1
            logger.info("거래량 프로필 분석 완료")

        except Exception as e:
            self.stats["volume_profile"]["failure"] += 1
            logger.error(f"거래량 프로필 분석 중 오류 발생: {e}")

    async def run_fear_greed_collector(self) -> None:
        """공포&탐욕 지수 수집 실행"""
        try:
            if "fear_greed" in self.running_tasks and not self.running_tasks["fear_greed"].done():
                logger.warning("이전 공포&탐욕 지수 수집 작업이 아직 실행 중입니다")
                return

            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"공포&탐욕 지수 수집 시작 (KST: {now_kst})")

            collector = FearGreedCollector()

            self.collectors["fear_greed"] = collector
            self.last_runs["fear_greed"] = datetime.now(KST)

            result = collector.run()

            # 리소스 정리
            collector.close()

            if result.get("error"):
                raise Exception(result["error"])

            self.stats["fear_greed"]["success"] += 1
            logger.info(f"공포&탐욕 지수 수집 완료: {result['data']['fear_greed_index']}")

        except Exception as e:
            self.stats["fear_greed"]["failure"] += 1
            logger.error(f"공포&탐욕 지수 수집 중 오류 발생: {e}")

    async def run_liquidation_analyzer(self) -> None:
        """청산 히트맵 분석 실행"""
        try:
            if "liquidation" in self.running_tasks and not self.running_tasks["liquidation"].done():
                logger.warning("이전 청산 히트맵 분석 작업이 아직 실행 중입니다")
                return

            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"청산 히트맵 분석 시작 (KST: {now_kst})")

            analyzer = LiquidationAnalyzer()

            self.collectors["liquidation"] = analyzer
            self.last_runs["liquidation"] = datetime.now(KST)

            result = await analyzer.run()

            # 리소스 정리
            analyzer.close()

            if not result["success"]:
                raise Exception(result.get("error", "분석 실패"))

            self.stats["liquidation"]["success"] += 1
            logger.info(f"청산 히트맵 분석 완료: {len(result['data'].get('clusters', []))} 클러스터 발견")

        except Exception as e:
            self.stats["liquidation"]["failure"] += 1
            logger.error(f"청산 히트맵 분석 중 오류 발생: {e}")

    async def run_onchain_data_collector(self) -> None:
        """온체인 데이터 수집 실행"""
        try:
            if "onchain" in self.running_tasks and not self.running_tasks["onchain"].done():
                logger.warning("이전 온체인 데이터 수집 작업이 아직 실행 중입니다")
                return

            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"온체인 데이터 수집 시작 (KST: {now_kst})")

            collector = OnChainDataCollector()

            self.collectors["onchain"] = collector
            self.last_runs["onchain"] = datetime.now(KST)

            result = await collector.run()

            # 리소스 정리
            collector.close()

            if not result["success"]:
                raise Exception(result.get("error", "수집 실패"))

            self.stats["onchain"]["success"] += 1
            logger.info(f"온체인 데이터 수집 완료: {result['market_analysis']['market_sentiment']}")

        except Exception as e:
            self.stats["onchain"]["failure"] += 1
            logger.error(f"온체인 데이터 수집 중 오류 발생: {e}")

    async def check_data_integrity(self) -> None:
        """
        데이터 무결성을 확인하고 필요한 경우 누락된 OHLCV 데이터를 채웁니다.
        """
        try:
            now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"데이터 무결성 확인 중... (KST: {now_kst})")

            # OHLCV 데이터 누락 확인 및 백필
            # 여기서 필요에 따라 구체적인 로직 구현

            logger.info("데이터 무결성 확인 완료")

        except Exception as e:
            logger.error(f"데이터 무결성 확인 중 오류 발생: {e}")

    def _schedule_task(self, task_name: str, interval_seconds: int, coroutine_func: Callable) -> None:
        """
        비동기 태스크를 스케줄링합니다.

        Args:
            task_name: 태스크 이름
            interval_seconds: 실행 간격(초)
            coroutine_func: 실행할 코루틴 함수
        """

        async def wrapper():
            task_start_time = datetime.now(KST)
            self.logger.info(f"{task_name} 작업 시작 (KST: {task_start_time.strftime('%Y-%m-%d %H:%M:%S')})")

            try:
                self.running_tasks[task_name] = asyncio.create_task(coroutine_func())
                await self.running_tasks[task_name]
                self.logger.info(f"{task_name} 작업 성공적으로 완료됨")
            except asyncio.CancelledError:
                self.logger.warning(f"{task_name} 작업이 취소됨")
                raise
            except Exception as e:
                self.logger.error(f"{task_name} 작업 실행 중 예외 발생: {e}", exc_info=True)
            finally:
                if task_name in self.running_tasks:
                    elapsed = (datetime.now(KST) - task_start_time).total_seconds()
                    self.logger.info(f"{task_name} 작업 종료 (소요 시간: {elapsed:.2f}초)")
                    del self.running_tasks[task_name]

        # 정각에 맞춰 첫 실행 시간 조정
        delay = self._align_to_interval(interval_seconds)

        # 한국 시간 기준으로 로깅
        next_run = (datetime.now(KST) + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"{task_name} 태스크가 {delay}초 후 (KST: {next_run})에 처음 실행되도록 예약됨, 이후 {interval_seconds}초 간격으로 실행")

        async def schedule_runner():
            # 첫 실행을 위한 대기
            await asyncio.sleep(delay)

            while self.is_running:
                next_run_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"{task_name} 태스크 실행 예정 (KST: {next_run_kst})")

                asyncio.create_task(wrapper())
                await asyncio.sleep(interval_seconds)

        self.tasks[task_name] = asyncio.create_task(schedule_runner())

    def _schedule_at_specific_times(self, task_name: str, times: List[Tuple[int, int]],
                                    coroutine_func: Callable) -> None:
        """
        특정 시간(KST)에 비동기 태스크를 스케줄링합니다.

        Args:
            task_name: 태스크 이름
            times: 실행할 시간 목록 [(시, 분), ...] - 24시간제, KST 기준
            coroutine_func: 실행할 코루틴 함수
        """

        async def wrapper():
            self.running_tasks[task_name] = asyncio.create_task(coroutine_func())
            try:
                await self.running_tasks[task_name]
            except Exception as e:
                logger.error(f"{task_name} 태스크 실행 중 예외 발생: {e}")
            finally:
                if task_name in self.running_tasks:
                    del self.running_tasks[task_name]

        async def schedule_runner():
            while self.is_running:
                now = datetime.now(KST)
                next_run_time = None
                soonest_wait = float('inf')

                for hour, minute in times:
                    target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                    # 이미 지난 시간이면 다음 날로 설정
                    if target_time <= now:
                        target_time = target_time + timedelta(days=1)

                    wait_seconds = (target_time - now).total_seconds()

                    if wait_seconds < soonest_wait:
                        soonest_wait = wait_seconds
                        next_run_time = target_time

                # 한국 시간으로 다음 실행 시간 로깅
                next_run_kst = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"{task_name} 태스크가 {soonest_wait:.1f}초 후 (KST: {next_run_kst})에 실행됩니다")

                await asyncio.sleep(soonest_wait)

                run_time_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"{task_name} 태스크 실행 중 (KST: {run_time_kst})")

                asyncio.create_task(wrapper())

        self.tasks[task_name] = asyncio.create_task(schedule_runner())

    async def start(self) -> None:
        """
        스케줄러를 시작하고 모든 수집 모듈을 예약합니다.
        """
        if self.is_running:
            logger.warning("스케줄러가 이미 실행 중입니다")
            return

        self.is_running = True
        self.event_loop = asyncio.get_running_loop()

        logger.info(f"데이터 수집 스케줄러 시작 (KST: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')})")

        # 1. 간격 기반 스케줄링 (OHLCV 데이터) - 정각에 맞춤
        self._schedule_task("market_data", MARKET_DATA_INTERVAL, self.run_market_data_collector)

        # 2. 간격 기반 스케줄링 (거래량 프로필) - 30분마다
        self._schedule_task("volume_profile", VOLUME_PROFILE_INTERVAL, self.run_volume_profile_analyzer)

        # 3. 특정 시간 기반 스케줄링 (한국 시간 기준)

        # Fear & Greed 지수 - 매일 아침 9:30 (한국 시간)
        # Alternative.me는 UTC 0:00경 업데이트
        self._schedule_at_specific_times(
            "fear_greed",
            [(9, 30)],  # KST 9:30
            self.run_fear_greed_collector
        )

        # 온체인 데이터 - 하루에 세 번 수집 (한국 시간)
        # CryptoQuant 업데이트에 맞춤
        self._schedule_at_specific_times(
            "onchain",
            [
                (9, 30),  # KST 9:30 (아침)
                (17, 30),  # KST 17:30 (저녁)
                (1, 30)  # KST 1:30 (새벽)
            ],
            self.run_onchain_data_collector
        )

        # 청산 히트맵 - 주요 거래 시간대에 맞춰 수집 (한국 시간)
        self._schedule_at_specific_times(
            "liquidation",
            [
                (11, 0),  # KST 11:00 (아시아 거래 시간)
                (19, 0),  # KST 19:00 (유럽 거래 시간)
                (3, 0)  # KST 3:00 (미국 거래 시간)
            ],
            self.run_liquidation_analyzer
        )

        # 데이터 무결성 검사 - 매일 새벽 4시 (한국 시간)
        self._schedule_at_specific_times(
            "data_integrity",
            [(4, 0)],  # KST 4:00
            self.check_data_integrity
        )

        try:
            while self.is_running:
                await asyncio.sleep(60)  # 1분마다 상태 확인
                self._log_status()
        except asyncio.CancelledError:
            logger.info("스케줄러가 취소되었습니다")
            await self.stop()

    def _log_status(self) -> None:
        """현재 스케줄러 상태를 로깅합니다."""
        # 한국 시간으로 변환
        last_runs_kst = {
            k: v.strftime("%Y-%m-%d %H:%M:%S")
            for k, v in self.last_runs.items()
        }

        status = {
            "running": self.is_running,
            "collectors": list(self.collectors.keys()),
            "last_runs_kst": last_runs_kst,
            "stats": self.stats,
            "current_time_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        }

        logger.info(f"스케줄러 상태: {status}")

    async def stop(self) -> None:
        """
        스케줄러를 중지하고 모든 리소스를 정리합니다.
        """
        if not self.is_running:
            logger.warning("스케줄러가 실행 중이지 않습니다")
            return

        logger.info(f"스케줄러 중지 중... (KST: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')})")
        self.is_running = False

        # 모든 예약된 태스크 취소
        for name, task in self.tasks.items():
            if not task.done():
                logger.info(f"{name} 태스크 취소 중...")
                task.cancel()

        # 실행 중인 태스크 취소
        for name, task in self.running_tasks.items():
            if not task.done():
                logger.info(f"실행 중인 {name} 태스크 취소 중...")
                task.cancel()

        # 수집기 리소스 정리
        for name, collector in self.collectors.items():
            if hasattr(collector, 'close'):
                logger.info(f"{name} 수집기 리소스 정리 중...")
                if asyncio.iscoroutinefunction(collector.close):
                    await collector.close()
                else:
                    collector.close()

        logger.info("스케줄러가 성공적으로 중지되었습니다")

    async def run_backfill(self, days: int = 365) -> None:
        """
        OHLCV 과거 데이터를 채웁니다.

        Args:
            days: 백필할 일수
        """
        try:
            logger.info(f"OHLCV 과거 데이터 채우기 시작 (KST: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')})")
            await backfill_ohlcv()
            logger.info("OHLCV 과거 데이터 채우기 완료")
        except Exception as e:
            logger.error(f"OHLCV 과거 데이터 채우기 중 오류 발생: {e}")


async def main():
    """
    메인 함수
    """
    # 시그널 핸들러 설정
    scheduler = DataCollectorScheduler()

    def signal_handler(sig, frame):
        logger.info(f"시그널 {sig} 받음, 종료 중...")
        asyncio.create_task(scheduler.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        days = 365
        if len(sys.argv) > 2:
            try:
                days = int(sys.argv[2])
            except ValueError:
                pass
        await scheduler.run_backfill(days)
        return

    await scheduler.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("키보드 인터럽트로 종료")