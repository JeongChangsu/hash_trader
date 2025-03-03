# data_collectors/onchain_data_collector.py
"""
온체인 데이터 수집기

이 모듈은 CryptoQuant에서 비트코인 온체인 데이터를 수집합니다.
Exchange Reserve, Exchange Netflow, SOPR, MVRV Ratio 등
주요 온체인 지표를 수집하고 분석합니다.
수집된 데이터는 Redis 및 PostgreSQL에 저장됩니다.

한국 시간(KST) 기준으로 데이터를 수집하고 저장합니다.
"""

import time
import json
import pytz
import logging
import asyncio
import psycopg2

import numpy as np
import pandas as pd
import redis.asyncio as redis
import undetected_chromedriver as uc

from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from typing import Dict, List, Any, Optional, Tuple
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config.logging_config import configure_logging
from config.settings import REDIS_CONFIG, POSTGRES_CONFIG, KEY_ONCHAIN_METRICS

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')


class OnChainDataCollector:
    """
    비트코인 온체인 데이터 수집 및 분석 클래스
    """

    def __init__(self):
        """OnChainDataCollector 초기화"""
        self.logger = configure_logging("onchain_data_collector")

        # Redis 연결 정보
        self.redis_params = {k: v for k, v in REDIS_CONFIG.items() if k != 'decode_responses'}

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # Redis 스트림 키
        self.redis_stream = "data:onchain:latest"

        # 데이터 URL
        self.urls = {
            "exchange_flows": "https://cryptoquant.com/ko/asset/btc/chart/exchange-flows",
            "flow_indicator": "https://cryptoquant.com/ko/asset/btc/chart/flow-indicator",
            "miner_flows": "https://cryptoquant.com/ko/asset/btc/chart/miner-flows",
            "market_indicator": "https://cryptoquant.com/ko/asset/btc/chart/market-indicator",
            "market_data": "https://cryptoquant.com/ko/asset/btc/chart/market-data",
            "network_indicator": "https://cryptoquant.com/ko/asset/btc/chart/network-indicator"
        }

        # 지표 번역 딕셔너리
        self.translation_dict = {
            "거래소 보유량": "Exchange Reserve",
            "거래소 순입출금량 (Netflow)": "Exchange Netflow",
            "거래소 입금량 Top10": "Exchange Inflow Top10",
            "거래소 출금량 Top10": "Exchange Outflow Top10",
            "채굴자 포지션 지표 (MPI)": "Miner Position Index (MPI)",
            "거래소 고래 비율 (Exchange Whale Ratio)": "Exchange Whale Ratio",
            "채굴자 보유량": "Miner Reserve",
            "단기 보유자 SOPR": "Short Term Holder SOPR",
            "장기 보유자 SOPR": "Long Term Holder SOPR",
            "SOPR 보정값": "Adjusted SOPR",
            "SOPR 비율 (장기 보유자 SOPR/단기 보유자 SOPR)": "SOPR Ratio (LTH/STH)",
            "MVRV 비율": "MVRV Ratio",
            "코인베이스 프리미엄 지표": "Coinbase Premium",
            "미실현 순수익(NUPL)": "Net Unrealized Profit/Loss (NUPL)"
        }

        # 중요 온체인 지표
        self.key_metrics = KEY_ONCHAIN_METRICS

        # 각 지표별 중요 임계값 설정
        self.metric_thresholds = {
            "Exchange Reserve": {
                "decreasing_strongly": -0.5,  # 강한 감소 (매우 강세)
                "decreasing": -0.2,  # 감소 (강세)
                "neutral_low": -0.1,  # 약간 감소 (약세)
                "neutral_high": 0.1,  # 약간 증가 (약세)
                "increasing": 0.2,  # 증가 (약세)
                "increasing_strongly": 0.5  # 강한 증가 (매우 약세)
            },
            "Exchange Netflow": {
                "outflow_strongly": -1000,  # 강한 유출 (매우 강세)
                "outflow": -500,  # 유출 (강세)
                "neutral_low": -100,  # 약한 유출 (약세)
                "neutral_high": 100,  # 약한 유입 (약세)
                "inflow": 500,  # 유입 (약세)
                "inflow_strongly": 1000  # 강한 유입 (매우 약세)
            },
            "Miner Position Index (MPI)": {
                "low": -2,  # 낮음 (강세)
                "neutral_low": 0,  # 중립 하단 (중립)
                "neutral_high": 2,  # 중립 상단 (중립)
                "high": 4,  # 높음 (약세)
                "very_high": 6  # 매우 높음 (매우 약세)
            },
            "Adjusted SOPR": {
                "very_low": 0.94,  # 매우 낮음 (강한 바닥 신호)
                "low": 0.98,  # 낮음 (바닥 가능성)
                "neutral_low": 1.0,  # 중립 하단
                "neutral_high": 1.02,  # 중립 상단
                "high": 1.05,  # 높음 (과열 가능성)
                "very_high": 1.1  # 매우 높음 (강한 과열 신호)
            },
            "MVRV Ratio": {
                "very_low": 1,  # 매우 낮음 (강한 바닥 신호)
                "low": 1.5,  # 낮음 (바닥 가능성)
                "neutral_low": 2,  # 중립 하단
                "neutral_high": 3,  # 중립 상단
                "high": 3.5,  # 높음 (과열 가능성)
                "very_high": 4  # 매우 높음 (강한 과열 신호)
            }
        }

        self.initialize_db()
        self.logger.info("OnChainDataCollector 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS onchain_data (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,  -- 날짜_시간 형식 (YYYYMMDD_HH)
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (collection_id)
                );

                CREATE TABLE IF NOT EXISTS onchain_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,  -- 날짜_시간 형식 (YYYYMMDD_HH)
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value NUMERIC NOT NULL,
                    change_24h NUMERIC,
                    signal VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (collection_id) REFERENCES onchain_data(collection_id) ON DELETE CASCADE,
                    UNIQUE (collection_id, metric_name)
                );

                CREATE INDEX IF NOT EXISTS idx_onchain_data_collection_id 
                ON onchain_data(collection_id);

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_timestamp 
                ON onchain_metrics(timestamp_ms);

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_name 
                ON onchain_metrics(metric_name);

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_collection_id 
                ON onchain_metrics(collection_id);
            """)
            self.db_conn.commit()
            self.logger.info("온체인 데이터 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def generate_collection_id(self) -> str:
        """
        수집 ID를 생성합니다. 날짜_시간 형식 (YYYYMMDD_HH)으로,
        시간은 9시, 17시, 1시 중 가장 가까운 시간대로 고정됩니다.

        Returns:
            str: 수집 ID
        """
        # 현재 한국 시간
        now = datetime.now(KST)

        # 시간대 결정 (9시, 17시, 1시 중 가장 가까운 시간)
        hour = now.hour

        if 1 <= hour < 13:
            target_hour = 9  # 오전 9시 (한국 시간)
        elif 13 <= hour < 21:
            target_hour = 17  # 오후 5시 (한국 시간)
        else:  # 21 <= hour < 24 또는 hour = 0
            target_hour = 1  # 오전 1시 (한국 시간)
            # 1시의 경우 자정 이후인지 확인
            if hour >= 21:
                now = now + timedelta(days=1)

        # YYYYMMDD_HH 형식으로 ID 생성
        collection_id = now.strftime('%Y%m%d') + f'_{target_hour:02d}'

        return collection_id

    def round_numeric(self, value: str) -> float:
        """
        문자열을 숫자로 변환하고 반올림합니다.

        Args:
            value: 변환할 문자열

        Returns:
            float: 변환된 숫자
        """
        try:
            # 천 단위 구분자(,) 제거 후 변환
            return round(float(value.replace(',', '')), 4)
        except (ValueError, AttributeError):
            return 0.0

    async def fetch_data(
            self,
            driver: uc.Chrome,
            url: str,
            indicators: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        지정된 URL에서 온체인 지표 데이터를 가져옵니다.

        Args:
            driver: Chrome 웹드라이버 인스턴스
            url: 데이터를 가져올 URL
            indicators: 가져올 지표 이름 목록

        Returns:
            Dict: 수집된 지표 데이터
        """
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"URL에서 데이터 가져오는 중: {url} (KST: {now_kst})")

        driver.get(url)
        await asyncio.sleep(5)  # 페이지 로딩 대기

        try:
            # 테이블 로딩 대기
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.XPATH, '//tbody[@class="ant-table-tbody"]')))

            result = {}
            tr_elements = driver.find_elements(By.XPATH,
                                               '//tbody[@class="ant-table-tbody"]//tr[contains(@class, "ant-table-row")]')

            for tr_element in tr_elements:
                td_elements = tr_element.find_elements(By.XPATH, './/td')[:4]

                if len(td_elements) < 4:
                    continue

                indicator_name = td_elements[0].text.strip()

                # 중요 지표만 필터링
                if indicator_name in indicators:
                    latest_value = self.round_numeric(td_elements[2].text.strip())
                    change_24h = self.round_numeric(td_elements[3].text.strip())

                    # 영문 지표명으로 변환
                    eng_indicator_name = self.translation_dict.get(indicator_name, indicator_name)

                    # 신호 생성
                    signal = self.generate_signal(eng_indicator_name, latest_value, change_24h)

                    result[eng_indicator_name] = {
                        "latest_value": latest_value,
                        "24h_change": change_24h,
                        "signal": signal
                    }

                    self.logger.info(f"지표 수집됨: {eng_indicator_name}={latest_value} (변화율: {change_24h}%)")

            return result

        except Exception as e:
            self.logger.error(f"데이터 가져오기 실패: {url} - {e}")
            return {}

    def generate_signal(
            self,
            metric_name: str,
            value: float,
            change_24h: float
    ) -> str:
        """
        지표 값에 따른 시장 신호를 생성합니다.

        Args:
            metric_name: 지표 이름
            value: 지표 값
            change_24h: 24시간 변화율

        Returns:
            str: 생성된 신호
        """
        # 기본 신호는 중립
        signal = "neutral"

        # 지표별 임계값이 있는 경우 적용
        if metric_name in self.metric_thresholds:
            thresholds = self.metric_thresholds[metric_name]

            if metric_name == "Exchange Reserve":
                # 거래소 보유량은 변화율에 따라 신호 생성
                if change_24h <= thresholds["decreasing_strongly"]:
                    signal = "very_bullish"
                elif change_24h <= thresholds["decreasing"]:
                    signal = "bullish"
                elif change_24h <= thresholds["neutral_low"]:
                    signal = "slightly_bullish"
                elif change_24h >= thresholds["increasing_strongly"]:
                    signal = "very_bearish"
                elif change_24h >= thresholds["increasing"]:
                    signal = "bearish"
                elif change_24h >= thresholds["neutral_high"]:
                    signal = "slightly_bearish"

            elif metric_name == "Exchange Netflow":
                # 순입출금량은 절대값에 따라 신호 생성
                if value <= thresholds["outflow_strongly"]:
                    signal = "very_bullish"
                elif value <= thresholds["outflow"]:
                    signal = "bullish"
                elif value <= thresholds["neutral_low"]:
                    signal = "slightly_bullish"
                elif value >= thresholds["inflow_strongly"]:
                    signal = "very_bearish"
                elif value >= thresholds["inflow"]:
                    signal = "bearish"
                elif value >= thresholds["neutral_high"]:
                    signal = "slightly_bearish"

            elif metric_name == "Miner Position Index (MPI)":
                # MPI는 절대값에 따라 신호 생성
                if value <= thresholds["low"]:
                    signal = "bullish"
                elif value <= thresholds["neutral_low"]:
                    signal = "slightly_bullish"
                elif value >= thresholds["very_high"]:
                    signal = "very_bearish"
                elif value >= thresholds["high"]:
                    signal = "bearish"
                elif value >= thresholds["neutral_high"]:
                    signal = "slightly_bearish"

            elif metric_name == "Adjusted SOPR":
                # SOPR는 1을 기준으로 신호 생성
                if value <= thresholds["very_low"]:
                    signal = "very_bullish"
                elif value <= thresholds["low"]:
                    signal = "bullish"
                elif value <= thresholds["neutral_low"]:
                    signal = "slightly_bullish"
                elif value >= thresholds["very_high"]:
                    signal = "very_bearish"
                elif value >= thresholds["high"]:
                    signal = "bearish"
                elif value >= thresholds["neutral_high"]:
                    signal = "slightly_bearish"

            elif metric_name == "MVRV Ratio":
                # MVRV 비율은 임계값에 따라 신호 생성
                if value <= thresholds["very_low"]:
                    signal = "very_bullish"
                elif value <= thresholds["low"]:
                    signal = "bullish"
                elif value <= thresholds["neutral_low"]:
                    signal = "slightly_bullish"
                elif value >= thresholds["very_high"]:
                    signal = "very_bearish"
                elif value >= thresholds["high"]:
                    signal = "bearish"
                elif value >= thresholds["neutral_high"]:
                    signal = "slightly_bearish"

        elif metric_name == "Exchange Inflow Top10":
            # 입금량 Top10은 높을수록 약세
            if value > 1000:
                signal = "very_bearish"
            elif value > 500:
                signal = "bearish"
            elif value > 200:
                signal = "slightly_bearish"

        elif metric_name == "Exchange Outflow Top10":
            # 출금량 Top10은 높을수록 강세
            if value > 1000:
                signal = "very_bullish"
            elif value > 500:
                signal = "bullish"
            elif value > 200:
                signal = "slightly_bullish"

        elif metric_name == "Net Unrealized Profit/Loss (NUPL)":
            # NUPL은 값이 클수록 약세
            if value < 0:
                signal = "very_bullish"  # 손실 상태 (바닥 신호)
            elif value < 0.25:
                signal = "bullish"
            elif value < 0.5:
                signal = "slightly_bullish"
            elif value > 0.75:
                signal = "bearish"
            elif value > 0.6:
                signal = "slightly_bearish"

        return signal

    async def gather_all_data(self) -> Dict[str, Dict[str, Any]]:
        """
        모든 온체인 데이터를 수집합니다.

        Returns:
            Dict: 수집된 모든 온체인 데이터
        """
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"온체인 데이터 수집 시작 (KST: {now_kst})")

        # Chrome 옵션 설정
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = None
        data = {}

        try:
            # Undetected Chrome 드라이버 초기화
            driver = uc.Chrome(options=options)

            # 각 URL에서 데이터 수집
            data.update(await self.fetch_data(driver, self.urls["exchange_flows"], [
                "거래소 보유량", "거래소 순입출금량 (Netflow)",
                "거래소 입금량 Top10", "거래소 출금량 Top10"
            ]))

            data.update(await self.fetch_data(driver, self.urls["flow_indicator"], [
                "채굴자 포지션 지표 (MPI)", "거래소 고래 비율 (Exchange Whale Ratio)"
            ]))

            data.update(await self.fetch_data(driver, self.urls["miner_flows"], [
                "채굴자 보유량"
            ]))

            data.update(await self.fetch_data(driver, self.urls["market_indicator"], [
                "단기 보유자 SOPR", "장기 보유자 SOPR", "SOPR 보정값",
                "SOPR 비율 (장기 보유자 SOPR/단기 보유자 SOPR)", "MVRV 비율"
            ]))

            data.update(await self.fetch_data(driver, self.urls["market_data"], [
                "코인베이스 프리미엄 지표"
            ]))

            data.update(await self.fetch_data(driver, self.urls["network_indicator"], [
                "미실현 순수익(NUPL)"
            ]))

            # 중요 지표만 필터링
            filtered_data = {}
            for metric in self.key_metrics:
                if metric in data:
                    filtered_data[metric] = data[metric]

            self.logger.info(f"온체인 데이터 수집 완료: {len(filtered_data)} 지표")
            return filtered_data

        except Exception as e:
            self.logger.error(f"온체인 데이터 수집 중 오류 발생: {e}")
            return data

        finally:
            if driver:
                driver.quit()

    async def save_results(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        수집된 온체인 데이터를 저장합니다.

        Args:
            data: 저장할 온체인 데이터

        Returns:
            Dict: 저장된 결과 정보
        """
        # 타임스탬프 및 collection_id 생성
        timestamp_ms = int(time.time() * 1000)
        collection_id = self.generate_collection_id()

        # 한국 시간 정보
        now_kst = datetime.now(KST)
        kst_date = now_kst.strftime("%Y-%m-%d")
        kst_time = now_kst.strftime("%H:%M:%S")

        # 데이터에 시간 정보 추가
        data_with_timestamp = {
            "timestamp_ms": timestamp_ms,
            "collection_id": collection_id,
            "kst_date": kst_date,
            "kst_time": kst_time,
            "data": data
        }

        # Redis 연결
        redis_client = await redis.Redis(**self.redis_params)

        try:
            # Redis 스트림에 저장
            await redis_client.xadd(
                self.redis_stream,
                {'data': json.dumps(data_with_timestamp, default=str)},
                maxlen=5000,
                approximate=True
            )

            # 최신 데이터로 설정
            await redis_client.set(
                "data:onchain:latest",
                json.dumps(data_with_timestamp, default=str)
            )

            self.logger.info(f"온체인 데이터가 Redis에 저장됨 (Collection ID: {collection_id})")

        except Exception as e:
            self.logger.error(f"Redis 저장 중 오류 발생: {e}")

        finally:
            await redis_client.close()

        # PostgreSQL에 저장
        cursor = self.db_conn.cursor()
        try:
            # 메인 온체인 데이터
            cursor.execute(
                """
                INSERT INTO onchain_data (timestamp_ms, collection_id, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (collection_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    timestamp_ms = EXCLUDED.timestamp_ms,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id;
                """,
                (timestamp_ms, collection_id, json.dumps(data_with_timestamp))
            )

            # 기존 지표 삭제 (같은 collection_id에 해당하는)
            cursor.execute(
                """
                DELETE FROM onchain_metrics
                WHERE collection_id = %s
                """,
                (collection_id,)
            )
            self.db_conn.commit()

            # 개별 지표 저장
            for metric_name, metric_data in data.items():
                cursor.execute(
                    """
                    INSERT INTO onchain_metrics 
                    (timestamp_ms, collection_id, metric_name, metric_value, change_24h, signal)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        timestamp_ms,
                        collection_id,
                        metric_name,
                        metric_data.get('latest_value', 0),
                        metric_data.get('24h_change', 0),
                        metric_data.get('signal', 'neutral')
                    )
                )

            self.db_conn.commit()
            self.logger.info(f"온체인 데이터가 PostgreSQL에 저장됨 (Collection ID: {collection_id})")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"PostgreSQL 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

        return {
            "timestamp_ms": timestamp_ms,
            "collection_id": collection_id,
            "kst_date": kst_date,
            "kst_time": kst_time
        }

    def analyze_market_situation(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        온체인 데이터를 기반으로 시장 상황을 분석합니다.

        Args:
            data: 분석할 온체인 데이터

        Returns:
            Dict: 시장 상황 분석 결과
        """
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"온체인 데이터 기반 시장 상황 분석 중 (KST: {now_kst})")

        # 신호 계수
        signal_counts = {
            "very_bullish": 0,
            "bullish": 0,
            "slightly_bullish": 0,
            "neutral": 0,
            "slightly_bearish": 0,
            "bearish": 0,
            "very_bearish": 0
        }

        # 신호별 가중치
        signal_weights = {
            "very_bullish": 3,
            "bullish": 2,
            "slightly_bullish": 1,
            "neutral": 0,
            "slightly_bearish": -1,
            "bearish": -2,
            "very_bearish": -3
        }

        # 지표별 가중치
        metric_weights = {
            "Exchange Reserve": 3,
            "Exchange Netflow": 3,
            "Exchange Inflow Top10": 2,
            "Exchange Outflow Top10": 2,
            "Miner Position Index (MPI)": 2,
            "Exchange Whale Ratio": 2,
            "Adjusted SOPR": 3,
            "MVRV Ratio": 3,
            "Net Unrealized Profit/Loss (NUPL)": 3,
            "Coinbase Premium": 1,
            "Miner Reserve": 2,
            "SOPR Ratio (LTH/STH)": 2,
            "Short Term Holder SOPR": 2,
            "Long Term Holder SOPR": 2
        }

        total_weight = 0
        weighted_score = 0

        # 수집된 지표별로 신호 분석
        for metric_name, metric_data in data.items():
            signal = metric_data.get('signal', 'neutral')

            # 신호 카운트 증가
            signal_counts[signal] = signal_counts.get(signal, 0) + 1

            # 가중 점수 계산
            metric_weight = metric_weights.get(metric_name, 1)
            signal_score = signal_weights.get(signal, 0)
            weighted_score += signal_score * metric_weight
            total_weight += metric_weight

        # 전체 가중 점수 계산
        overall_score = 0
        if total_weight > 0:
            overall_score = weighted_score / total_weight

        # 시장 분위기 판단
        market_sentiment = "neutral"
        if overall_score <= -2:
            market_sentiment = "very_bearish"
        elif overall_score <= -1:
            market_sentiment = "bearish"
        elif overall_score <= -0.3:
            market_sentiment = "slightly_bearish"
        elif overall_score >= 2:
            market_sentiment = "very_bullish"
        elif overall_score >= 1:
            market_sentiment = "bullish"
        elif overall_score >= 0.3:
            market_sentiment = "slightly_bullish"

        # 불확실성 수준 계산 (신호가 일관될수록 낮음)
        uncertainty_level = "medium"
        signal_variety = len([count for count in signal_counts.values() if count > 0])
        if signal_variety <= 2:
            uncertainty_level = "low"
        elif signal_variety >= 5:
            uncertainty_level = "high"

        # 심각한 신호 추출
        strong_bullish_signals = [metric for metric, data in data.items()
                                  if data.get('signal') in ['very_bullish', 'bullish']]

        strong_bearish_signals = [metric for metric, data in data.items()
                                  if data.get('signal') in ['very_bearish', 'bearish']]

        # 한국 시간 정보 추가
        collection_id = self.generate_collection_id()
        now_kst = datetime.now(KST)

        return {
            "timestamp_ms": int(time.time() * 1000),
            "collection_id": collection_id,
            "kst_date": now_kst.strftime("%Y-%m-%d"),
            "kst_time": now_kst.strftime("%H:%M:%S"),
            "overall_score": round(overall_score, 2),
            "market_sentiment": market_sentiment,
            "uncertainty_level": uncertainty_level,
            "signal_counts": signal_counts,
            "strong_bullish_signals": strong_bullish_signals,
            "strong_bearish_signals": strong_bearish_signals
        }

    async def run(self) -> Dict[str, Any]:
        """
        온체인 데이터 수집 및 분석을 실행합니다.

        Returns:
            Dict: 수집 및 분석 결과
        """
        # 한국 시간으로 현재 시간 기록
        now_kst = datetime.now(KST)
        collection_id = self.generate_collection_id()

        result = {
            "success": False,
            "timestamp_ms": int(time.time() * 1000),
            "kst_time": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
            "kst_date": now_kst.strftime("%Y-%m-%d"),
            "collection_id": collection_id
        }

        try:
            self.logger.info(f"온체인 데이터 수집 및 분석 시작 (KST: {now_kst.strftime('%Y-%m-%d %H:%M:%S')})")

            # 데이터 수집
            data = await self.gather_all_data()

            if not data:
                raise Exception("수집된 온체인 데이터가 없습니다")

            # 데이터 저장
            storage_info = await self.save_results(data)

            # 시장 상황 분석
            market_analysis = self.analyze_market_situation(data)

            # 결과에 데이터 추가
            result.update({
                "success": True,
                "storage_info": storage_info,
                "data": data,
                "market_analysis": market_analysis
            })

            self.logger.info(
                f"온체인 데이터 수집 및 분석 완료 (Collection ID: {collection_id}): {market_analysis['market_sentiment']}")

        except Exception as e:
            self.logger.error(f"온체인 데이터 수집 및 분석 중 오류 발생: {e}")
            result["error"] = str(e)

        return result

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")


# 직접 실행 시 수집기 시작
if __name__ == "__main__":
    collector = OnChainDataCollector()

    try:
        # 비동기 실행을 위한 이벤트 루프
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(collector.run())

        if result["success"]:
            print(f"수집 완료. Collection ID: {result['collection_id']}")
            print(f"지표 수: {len(result['data'])}")
            print(f"시장 분위기: {result['market_analysis']['market_sentiment']}")
            print(f"전체 점수: {result['market_analysis']['overall_score']}")
        else:
            print(f"수집 실패: {result.get('error', '알 수 없는 오류')}")

    except Exception as e:
        print(f"실행 중 오류 발생: {e}")
    finally:
        collector.close()
        if 'loop' in locals():
            loop.close()
