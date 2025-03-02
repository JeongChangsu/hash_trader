# data_collectors/fear_greed_collector.py
"""
공포&탐욕 지수 수집기

이 모듈은 Bitcoin Fear & Greed Index를 수집하고 분석합니다.
일일 지수 값과 추세를 수집하고, 극단적인 값에 대한 신호를 생성합니다.
수집된 데이터는 Redis 및 PostgreSQL에 저장됩니다.
"""

import requests
import redis
import psycopg2
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

from config.settings import REDIS_CONFIG, POSTGRES_CONFIG
from config.logging_config import configure_logging


class FearGreedCollector:
    """
    Bitcoin Fear & Greed Index 수집 및 분석 클래스
    """

    def __init__(self):
        """FearGreedCollector 초기화"""
        self.logger = configure_logging("fear_greed_collector")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # API URL
        self.api_url = "https://api.alternative.me/fng/"

        # 감정 카테고리 매핑
        self.sentiment_categories = {
            "Extreme Fear": {"min": 0, "max": 24, "signal": "potential_buy"},
            "Fear": {"min": 25, "max": 49, "signal": "cautious_buy"},
            "Neutral": {"min": 50, "max": 74, "signal": "neutral"},
            "Greed": {"min": 75, "max": 89, "signal": "cautious_sell"},
            "Extreme Greed": {"min": 90, "max": 100, "signal": "potential_sell"}
        }

        self.initialize_db()
        self.logger.info("FearGreedCollector 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블 초기화"""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fear_greed_index (
                    timestamp_ms BIGINT PRIMARY KEY,
                    fear_greed_index INTEGER,
                    sentiment_category VARCHAR(50),
                    change_rate FLOAT,
                    date_value DATE,
                    signal VARCHAR(50)
                );

                CREATE INDEX IF NOT EXISTS idx_fear_greed_date 
                ON fear_greed_index (date_value);
            """)
            self.db_conn.commit()
            self.logger.info("Fear & Greed Index 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def fetch_fear_greed_index(self, limit: int = 2) -> Dict[str, Any]:
        """
        Fear & Greed Index를 가져옵니다.

        Args:
            limit: 가져올 데이터 수 (기본값: 2, 최신 및 이전 데이터)

        Returns:
            Dict: 수집된 Fear & Greed 데이터
        """
        self.logger.info(f"Fear & Greed Index 데이터 {limit}개 가져오는 중")

        response = requests.get(self.api_url, params={"limit": limit, "format": "json"})
        response.raise_for_status()
        data = response.json()['data']

        latest = data[0]
        previous = data[1] if len(data) > 1 else None

        latest_value = int(latest['value'])
        previous_value = int(previous['value']) if previous else latest_value

        change_rate = ((latest_value - previous_value) / previous_value) * 100 if previous else 0

        # 날짜 형식 변환
        timestamp_ms = int(latest['timestamp']) * 1000
        date_value = datetime.fromtimestamp(int(latest['timestamp'])).date()

        # 감정 카테고리에 따른 신호 생성
        sentiment = latest['value_classification']
        signal = self.get_signal_from_sentiment(sentiment)

        return {
            "timestamp_ms": timestamp_ms,
            "fear_greed_index": latest_value,
            "sentiment_category": sentiment,
            "change_rate": round(change_rate, 2),
            "date_value": date_value.isoformat(),
            "signal": signal
        }

    def get_signal_from_sentiment(self, sentiment: str) -> str:
        """
        감정 카테고리에 따른 신호를 반환합니다.

        Args:
            sentiment: 감정 카테고리 문자열

        Returns:
            str: 트레이딩 신호
        """
        if sentiment in self.sentiment_categories:
            return self.sentiment_categories[sentiment]["signal"]
        return "neutral"

    def fetch_historical_data(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        과거 Fear & Greed Index 데이터를 가져옵니다.

        Args:
            days: 가져올 이전 일수

        Returns:
            List[Dict]: 과거 데이터 리스트
        """
        self.logger.info(f"과거 {days}일간의 Fear & Greed 데이터 가져오는 중")

        response = requests.get(self.api_url, params={"limit": days, "format": "json"})
        response.raise_for_status()
        data = response.json()['data']

        result = []
        for item in data:
            timestamp_ms = int(item['timestamp']) * 1000
            value = int(item['value'])
            sentiment = item['value_classification']
            date_value = datetime.fromtimestamp(int(item['timestamp'])).date()
            signal = self.get_signal_from_sentiment(sentiment)

            result.append({
                "timestamp_ms": timestamp_ms,
                "fear_greed_index": value,
                "sentiment_category": sentiment,
                "date_value": date_value.isoformat(),
                "signal": signal
            })

        return result

    def save_to_redis(self, data: Dict[str, Any]) -> None:
        """
        Fear & Greed 데이터를 Redis에 저장합니다.

        Args:
            data: 저장할 데이터
        """
        key = "data:fear_greed:latest"
        self.redis_client.set(key, json.dumps(data))
        self.redis_client.expire(key, 604800)  # 7일 후 만료

        # 스트림에도 추가
        stream_key = "stream:fear_greed"
        self.redis_client.xadd(stream_key, {"data": json.dumps(data)}, maxlen=1000)

        self.logger.info(f"Fear & Greed 데이터가 Redis에 저장됨: {data['fear_greed_index']}")

    def save_to_postgres(self, data: Dict[str, Any]) -> None:
        """
        Fear & Greed 데이터를 PostgreSQL에 저장합니다.

        Args:
            data: 저장할 데이터
        """
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO fear_greed_index 
                (timestamp_ms, fear_greed_index, sentiment_category, change_rate, date_value, signal)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp_ms) DO UPDATE SET
                    fear_greed_index = EXCLUDED.fear_greed_index,
                    sentiment_category = EXCLUDED.sentiment_category,
                    change_rate = EXCLUDED.change_rate,
                    date_value = EXCLUDED.date_value,
                    signal = EXCLUDED.signal;
                """,
                (
                    data['timestamp_ms'],
                    data['fear_greed_index'],
                    data['sentiment_category'],
                    data.get('change_rate', 0),
                    data['date_value'],
                    data['signal']
                )
            )
            self.db_conn.commit()
            self.logger.info(f"Fear & Greed 데이터가 PostgreSQL에 저장됨: {data['fear_greed_index']}")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"PostgreSQL 저장 중 오류 발생: {e}")
        finally:
            cursor.close()

    def analyze_fear_greed_trend(self, days: int = 30) -> Dict[str, Any]:
        """
        Fear & Greed Index 추세를 분석합니다.

        Args:
            days: 분석할 이전 일수

        Returns:
            Dict: 추세 분석 결과
        """
        self.logger.info(f"지난 {days}일간의 Fear & Greed 추세 분석 중")

        cursor = self.db_conn.cursor()
        try:
            # 지정된 일수만큼의 데이터 가져오기
            cursor.execute(
                """
                SELECT timestamp_ms, fear_greed_index, sentiment_category, date_value
                FROM fear_greed_index
                WHERE date_value >= %s
                ORDER BY timestamp_ms DESC
                """,
                ((datetime.now() - timedelta(days=days)).date().isoformat(),)
            )

            rows = cursor.fetchall()

            if not rows:
                self.logger.warning(f"분석할 데이터가 없습니다. 과거 {days}일간의 데이터 먼저 수집")
                historical_data = self.fetch_historical_data(days)
                for data in historical_data:
                    self.save_to_postgres(data)

                # 다시 데이터 가져오기
                cursor.execute(
                    """
                    SELECT timestamp_ms, fear_greed_index, sentiment_category, date_value
                    FROM fear_greed_index
                    WHERE date_value >= %s
                    ORDER BY timestamp_ms DESC
                    """,
                    ((datetime.now() - timedelta(days=days)).date().isoformat(),)
                )
                rows = cursor.fetchall()

            # 데이터 변환
            df = pd.DataFrame(rows, columns=['timestamp_ms', 'fear_greed_index', 'sentiment_category', 'date_value'])

            if df.empty:
                return {
                    "trend": "unknown",
                    "avg_index": 0,
                    "min_index": 0,
                    "max_index": 0,
                    "current_index": 0,
                    "extreme_values": [],
                    "days_analyzed": 0
                }

            # 추세 분석
            df['date_value'] = pd.to_datetime(df['date_value'])
            df = df.sort_values('date_value')

            # 평균, 최소, 최대값
            avg_index = df['fear_greed_index'].mean()
            min_index = df['fear_greed_index'].min()
            max_index = df['fear_greed_index'].max()
            current_index = df.iloc[-1]['fear_greed_index']

            # 트렌드 방향 결정 (선형 회귀 기울기 사용)
            x = np.arange(len(df))
            y = df['fear_greed_index'].values

            if len(x) > 1:
                slope = np.polyfit(x, y, 1)[0]

                if slope > 0.5:
                    trend = "strongly_increasing"
                elif slope > 0.1:
                    trend = "increasing"
                elif slope < -0.5:
                    trend = "strongly_decreasing"
                elif slope < -0.1:
                    trend = "decreasing"
                else:
                    trend = "neutral"
            else:
                trend = "unknown"

            # 극단적 값 식별 (25 이하 또는 75 이상)
            extreme_values = df[
                (df['fear_greed_index'] <= 25) | (df['fear_greed_index'] >= 75)
                ][['date_value', 'fear_greed_index', 'sentiment_category']].to_dict('records')

            return {
                "trend": trend,
                "avg_index": round(avg_index, 2),
                "min_index": int(min_index),
                "max_index": int(max_index),
                "current_index": int(current_index),
                "extreme_values": extreme_values,
                "days_analyzed": len(df)
            }

        except Exception as e:
            self.logger.error(f"추세 분석 중 오류 발생: {e}")
            return {
                "trend": "error",
                "error": str(e)
            }
        finally:
            cursor.close()

    def generate_fear_greed_signals(self) -> Dict[str, Any]:
        """
        Fear & Greed Index 기반 트레이딩 신호를 생성합니다.

        Returns:
            Dict: 생성된 트레이딩 신호
        """
        self.logger.info("Fear & Greed 기반 트레이딩 신호 생성 중")

        # 현재 및 과거 30일 데이터 분석
        current_data = self.fetch_fear_greed_index()
        trend_analysis = self.analyze_fear_greed_trend(30)

        current_index = current_data['fear_greed_index']
        current_sentiment = current_data['sentiment_category']
        trend = trend_analysis['trend']

        # 신호 생성 로직
        signal_strength = "none"
        signal_direction = "neutral"

        # 공포 구간에서의 신호
        if current_index <= 25:  # Extreme Fear
            signal_direction = "buy"

            if current_index <= 10:
                signal_strength = "very_strong"
            elif current_index <= 20:
                signal_strength = "strong"
            else:
                signal_strength = "moderate"

            # 추세가 감소 중이면 강도 약화
            if trend in ["decreasing", "strongly_decreasing"]:
                if signal_strength == "very_strong":
                    signal_strength = "strong"
                elif signal_strength == "strong":
                    signal_strength = "moderate"
                elif signal_strength == "moderate":
                    signal_strength = "weak"

        # 탐욕 구간에서의 신호
        elif current_index >= 75:  # Extreme Greed
            signal_direction = "sell"

            if current_index >= 90:
                signal_strength = "very_strong"
            elif current_index >= 80:
                signal_strength = "strong"
            else:
                signal_strength = "moderate"

            # 추세가 증가 중이면 강도 약화
            if trend in ["increasing", "strongly_increasing"]:
                if signal_strength == "very_strong":
                    signal_strength = "strong"
                elif signal_strength == "strong":
                    signal_strength = "moderate"
                elif signal_strength == "moderate":
                    signal_strength = "weak"

        # 중립 구간에서의 신호
        else:
            if trend == "strongly_increasing" and current_index > 60:
                signal_direction = "cautious_sell"
                signal_strength = "weak"
            elif trend == "strongly_decreasing" and current_index < 40:
                signal_direction = "cautious_buy"
                signal_strength = "weak"
            else:
                signal_direction = "neutral"
                signal_strength = "none"

        signal = {
            "timestamp_ms": current_data['timestamp_ms'],
            "index": current_index,
            "sentiment": current_sentiment,
            "trend": trend,
            "signal_direction": signal_direction,
            "signal_strength": signal_strength,
            "analysis": {
                "avg_30d": trend_analysis['avg_index'],
                "min_30d": trend_analysis['min_index'],
                "max_30d": trend_analysis['max_index']
            }
        }

        # Redis에 신호 저장
        self.redis_client.set("signal:fear_greed:latest", json.dumps(signal))

        return signal

    def run(self) -> Dict[str, Any]:
        """
        Fear & Greed 데이터 수집 및 분석을 실행합니다.

        Returns:
            Dict: 수집된 데이터 및 생성된 신호
        """
        self.logger.info("Fear & Greed Index 수집 및 분석 시작")

        try:
            # 데이터 수집
            data = self.fetch_fear_greed_index()

            # 데이터 저장
            self.save_to_redis(data)
            self.save_to_postgres(data)

            # 신호 생성
            signals = self.generate_fear_greed_signals()

            self.logger.info(
                f"Fear & Greed 수집 및 분석 완료: 지수={data['fear_greed_index']}, 신호={signals['signal_direction']}")

            return {
                "data": data,
                "signals": signals
            }
        except Exception as e:
            self.logger.error(f"Fear & Greed 수집 및 분석 중 오류 발생: {e}")
            return {
                "error": str(e)
            }
        finally:
            # PostgreSQL 연결 유지 (다른 메서드에서 재사용)
            pass

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")

        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            self.logger.info("Redis 연결 종료됨")


# 직접 실행 시 데이터 수집기 시작
if __name__ == "__main__":
    collector = FearGreedCollector()

    try:
        result = collector.run()
        print(f"Fear & Greed Index: {result['data']['fear_greed_index']}")
        print(f"Signal: {result['signals']['signal_direction']} ({result['signals']['signal_strength']})")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        collector.close()
