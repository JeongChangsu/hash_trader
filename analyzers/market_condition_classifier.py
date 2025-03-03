# analyzers/market_condition_classifier.py
"""
시장 상황 분류기

이 모듈은 다양한 시장 데이터를 기반으로 시장 상황을 분류합니다.
시장 상황(추세, 레인지, 변동성 등)과 국면(상승, 하락, 횡보 등)을
식별하고, 각 상황에 적합한 전략을 추천합니다.
"""

import logging
import numpy as np
import pandas as pd
import redis
import psycopg2
import json
import time
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta
from scipy.stats import linregress

from config.settings import POSTGRES_CONFIG, REDIS_CONFIG
from config.logging_config import configure_logging


class MarketConditionClassifier:
    """
    시장 상황을 분류하고 분석하는 클래스
    """

    def __init__(self):
        """MarketConditionClassifier 초기화"""
        self.logger = configure_logging("market_condition_classifier")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 시장 상황 분류
        self.market_conditions = {
            'trending_up': '상승 추세',
            'trending_down': '하락 추세',
            'ranging': '횡보장',
            'high_volatility': '고변동성',
            'low_volatility': '저변동성',
            'accumulation': '누적 구간',
            'distribution': '배분 구간',
            'breakout': '돌파',
            'breakdown': '하방 돌파',
            'consolidation': '조정 구간'
        }

        # 시장 상황별 적합한 전략
        self.condition_strategies = {
            'trending_up': ['trend_following', 'breakout'],
            'trending_down': ['trend_following', 'breakdown'],
            'ranging': ['range', 'mean_reversion'],
            'high_volatility': ['breakout', 'options_strategies'],
            'low_volatility': ['range', 'accumulation'],
            'accumulation': ['accumulation', 'breakout'],
            'distribution': ['distribution', 'breakdown'],
            'breakout': ['breakout', 'trend_following'],
            'breakdown': ['breakdown', 'trend_following'],
            'consolidation': ['range', 'accumulation']
        }

        self.initialize_db()
        self.logger.info("MarketConditionClassifier 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_conditions (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    primary_condition VARCHAR(50) NOT NULL,
                    secondary_condition VARCHAR(50),
                    market_phase VARCHAR(50) NOT NULL,
                    volatility_level FLOAT NOT NULL,
                    trend_strength FLOAT NOT NULL,
                    recommended_strategies JSONB NOT NULL,
                    classification_confidence FLOAT NOT NULL,
                    data_sources JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_market_conditions_lookup 
                ON market_conditions (symbol, timestamp_ms);
            """)
            self.db_conn.commit()
            self.logger.info("시장 상황 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def fetch_ohlcv_data(
            self,
            symbol: str,
            timeframe: str = '1d',
            limit: int = 100
    ) -> pd.DataFrame:
        """
        지정된 심볼과 시간프레임에 대한 OHLCV 데이터를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            timeframe: 시간프레임 (예: '1d')
            limit: 가져올 캔들 수

        Returns:
            pd.DataFrame: OHLCV 데이터프레임
        """
        self.logger.info(f"OHLCV 데이터 가져오는 중: {symbol} ({timeframe}), {limit}개 캔들")

        try:
            cursor = self.db_conn.cursor()

            # 심볼을 데이터베이스 형식에 맞게 변환
            db_symbol = symbol

            cursor.execute(
                """
                SELECT 
                    timestamp_ms, open, high, low, close, volume
                FROM 
                    market_ohlcv
                WHERE 
                    symbol = %s AND timeframe = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT %s
                """,
                (db_symbol, timeframe, limit)
            )

            rows = cursor.fetchall()

            if not rows:
                self.logger.warning(f"OHLCV 데이터가 없습니다: {symbol} ({timeframe})")
                return pd.DataFrame()

            # 데이터프레임 생성 및 정렬
            df = pd.DataFrame(
                rows,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

            # 모든 가격 관련 컬럼을 명시적으로 numeric 타입으로 강제 변환
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # NaN 값 제거 (매우 중요)
            df = df.dropna(subset=numeric_cols)

            # 타임스탬프 변환 및 정렬
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp').reset_index(drop=True)

            # 가격 변화율 계산
            df['returns'] = df['close'].pct_change()
            df['log_returns'] = np.log(df['close'] / df['close'].shift(1))

            # 진폭(범위) 계산
            df['range'] = df['high'] - df['low']
            df['range_pct'] = df['range'] / df['close'].shift(1) * 100

            # 볼륨 변화율
            df['volume_change'] = df['volume'].pct_change()

            # 이동평균 계산
            df['sma20'] = df['close'].rolling(window=20).mean()
            df['sma50'] = df['close'].rolling(window=50).mean()
            df['sma200'] = df['close'].rolling(window=200).mean()

            # RSI 계산 (14일)
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            df['rsi'] = 100 - (100 / (1 + rs))

            # ATR 계산 (14일)
            tr1 = df['high'] - df['low']
            tr2 = abs(df['high'] - df['close'].shift(1))
            tr3 = abs(df['low'] - df['close'].shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['atr'] = tr.rolling(window=14).mean()
            df['atr_pct'] = df['atr'] / df['close'] * 100

            self.logger.info(f"OHLCV 데이터 로드됨: {len(df)} 캔들")
            return df

        except Exception as e:
            self.logger.error(f"OHLCV 데이터 가져오기 실패: {e}")
            return pd.DataFrame()
        finally:
            if 'cursor' in locals():
                cursor.close()

    def fetch_multi_timeframe_data(
            self,
            symbol: str,
            timeframes: List[str] = ['1h', '4h', '1d', '1w'],
            limit: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        여러 시간프레임에 대한 OHLCV 데이터를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            timeframes: 시간프레임 목록
            limit: 가져올 캔들 수

        Returns:
            Dict[str, pd.DataFrame]: 시간프레임별 OHLCV 데이터프레임
        """
        result = {}

        for tf in timeframes:
            df = self.fetch_ohlcv_data(symbol, tf, limit)
            if not df.empty:
                result[tf] = df

        return result

    def fetch_onchain_data(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        최신 온체인 데이터를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 온체인 데이터
        """
        self.logger.info(f"온체인 데이터 가져오는 중: {symbol}")

        # Redis에서 최신 온체인 데이터 가져오기
        redis_key = "data:onchain:latest"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error(f"온체인 데이터 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    data
                FROM 
                    onchain_data
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            else:
                self.logger.warning("온체인 데이터가 없습니다")
                return {}

        except Exception as e:
            self.logger.error(f"온체인 데이터 가져오기 실패: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def fetch_fear_greed_data(self) -> Dict[str, Any]:
        """
        최신 공포&탐욕 지수 데이터를 가져옵니다.

        Returns:
            Dict: 공포&탐욕 지수 데이터
        """
        self.logger.info("공포&탐욕 지수 데이터 가져오는 중")

        # Redis에서 최신 데이터 가져오기
        redis_key = "data:fear_greed:latest"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error("공포&탐욕 데이터 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    fear_greed_index, sentiment_category, signal
                FROM 
                    fear_greed_index
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return {
                    'fear_greed_index': row[0],
                    'sentiment_category': row[1],
                    'signal': row[2]
                }
            else:
                self.logger.warning("공포&탐욕 데이터가 없습니다")
                return {}

        except Exception as e:
            self.logger.error(f"공포&탐욕 데이터 가져오기 실패: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def calculate_market_volatility(
            self,
            df: pd.DataFrame,
            window: int = 14
    ) -> Tuple[float, str]:
        """
        시장 변동성을 계산합니다.

        Args:
            df: OHLCV 데이터프레임
            window: 변동성 계산 윈도우

        Returns:
            Tuple[float, str]: (변동성 수치, 변동성 수준)
        """
        if df.empty or len(df) < window:
            return 0.0, "unknown"

        # ATR 기반 변동성 (최근 n일 평균)
        recent_atr_pct = df['atr_pct'].iloc[-window:].mean()

        # 일일 범위 변동성 (최근 n일 평균)
        recent_range_pct = df['range_pct'].iloc[-window:].mean()

        # 일일 수익률 표준편차 (최근 n일)
        returns_std = df['returns'].iloc[-window:].std() * 100  # 백분율로 변환

        # 종합 변동성 점수 (각 지표 가중 평균)
        volatility_score = (recent_atr_pct * 0.4) + (recent_range_pct * 0.3) + (returns_std * 0.3)

        # 변동성 수준 분류
        if volatility_score < 2:
            volatility_level = "very_low"
        elif volatility_score < 3:
            volatility_level = "low"
        elif volatility_score < 5:
            volatility_level = "medium"
        elif volatility_score < 7:
            volatility_level = "high"
        else:
            volatility_level = "very_high"

        return volatility_score, volatility_level

    def calculate_trend_strength(
            self,
            df: pd.DataFrame,
            window: int = 20
    ) -> Tuple[float, str]:
        """
        추세 강도를 계산합니다.

        Args:
            df: OHLCV 데이터프레임
            window: 추세 계산 윈도우

        Returns:
            Tuple[float, str]: (추세 강도 수치, 추세 방향)
        """
        if df.empty or len(df) < window:
            return 0.0, "unknown"

        # 선형 회귀 기울기 계산
        recent_df = df.iloc[-window:]
        x = np.arange(len(recent_df))
        y = recent_df['close'].values

        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        # 기울기를 정규화 (가격 대비 백분율)
        normalized_slope = slope / recent_df['close'].mean() * 100

        # r-squared 값 (추세의 선형성)
        r_squared = r_value ** 2

        # 추세 강도 지표 (기울기 및 일관성 결합)
        trend_strength = abs(normalized_slope) * r_squared

        # 추세 방향
        if normalized_slope > 0.05 and r_squared > 0.6:
            trend_direction = "strongly_up"
        elif normalized_slope > 0.02 and r_squared > 0.4:
            trend_direction = "up"
        elif normalized_slope < -0.05 and r_squared > 0.6:
            trend_direction = "strongly_down"
        elif normalized_slope < -0.02 and r_squared > 0.4:
            trend_direction = "down"
        else:
            trend_direction = "neutral"

        # 이동평균 기반 추세 확인
        ma_trend = "unknown"

        if not pd.isna(df['sma20'].iloc[-1]) and not pd.isna(df['sma50'].iloc[-1]):
            current_price = df['close'].iloc[-1]
            sma20 = df['sma20'].iloc[-1]
            sma50 = df['sma50'].iloc[-1]

            if current_price > sma20 > sma50:
                ma_trend = "up"
            elif current_price < sma20 < sma50:
                ma_trend = "down"
            else:
                ma_trend = "mixed"

        # 추세 방향 조정 (이동평균 확인)
        if ma_trend == "up" and trend_direction in ["neutral", "up", "strongly_up"]:
            trend_direction = "up" if trend_direction == "neutral" else trend_direction
        elif ma_trend == "down" and trend_direction in ["neutral", "down", "strongly_down"]:
            trend_direction = "down" if trend_direction == "neutral" else trend_direction

        return trend_strength, trend_direction

    def detect_market_phases(
            self,
            df: pd.DataFrame,
            short_window: int = 20,
            long_window: int = 50
    ) -> str:
        """
        시장 국면을 감지합니다.

        Args:
            df: OHLCV 데이터프레임
            short_window: 단기 윈도우
            long_window: 장기 윈도우

        Returns:
            str: 시장 국면
        """
        if df.empty or len(df) < long_window:
            return "unknown"

        # 최근 가격 및 이동평균
        current_price = df['close'].iloc[-1]

        # 이동평균이 계산되지 않은 경우 직접 계산
        if pd.isna(df['sma20'].iloc[-1]):
            sma20 = df['close'].rolling(window=short_window).mean().iloc[-1]
        else:
            sma20 = df['sma20'].iloc[-1]

        if pd.isna(df['sma50'].iloc[-1]):
            sma50 = df['close'].rolling(window=long_window).mean().iloc[-1]
        else:
            sma50 = df['sma50'].iloc[-1]

        # 이전 기간 가격
        prev_price = df['close'].iloc[-short_window]
        price_change_pct = (current_price - prev_price) / prev_price * 100

        # 볼륨 변화
        recent_volume = df['volume'].iloc[-short_window:].mean()
        prev_volume = df['volume'].iloc[-long_window:-short_window].mean()
        volume_change_pct = (recent_volume - prev_volume) / prev_volume * 100

        # RSI 값
        current_rsi = df['rsi'].iloc[-1] if not pd.isna(df['rsi'].iloc[-1]) else 50

        # 시장 국면 판단
        if current_price > sma20 > sma50 and price_change_pct > 5:
            if current_rsi > 70:
                return "euphoria"  # 과열 상태
            else:
                return "advancing"  # 상승 중

        elif current_price < sma20 < sma50 and price_change_pct < -5:
            if current_rsi < 30:
                return "capitulation"  # 급락 상태
            else:
                return "declining"  # 하락 중

        elif abs(price_change_pct) < 3 and sma20 < current_price * 1.03 and sma20 > current_price * 0.97:
            if volume_change_pct < -20:
                return "accumulation"  # 누적 구간
            elif volume_change_pct > 20:
                return "distribution"  # 배분 구간
            else:
                return "consolidation"  # 조정 구간

        elif current_price > sma20 and sma20 < sma50:
            return "recovery"  # 회복 단계

        elif current_price < sma20 and sma20 > sma50:
            return "correction"  # 조정 단계

        else:
            return "indecisive"  # 불확실 구간

    def classify_market_condition(
            self,
            market_data: Dict[str, pd.DataFrame],
            onchain_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        시장 상황을 분류합니다.

        Args:
            market_data: 시간프레임별 OHLCV 데이터
            onchain_data: 온체인 데이터

        Returns:
            Dict: 시장 상황 분류 결과
        """
        self.logger.info("시장 상황 분류 시작")

        # 기본 결과 구조
        result = {
            'timestamp_ms': int(time.time() * 1000),
            'primary_condition': 'unknown',
            'secondary_condition': 'unknown',
            'market_phase': 'unknown',
            'volatility': {
                'score': 0.0,
                'level': 'unknown'
            },
            'trend': {
                'strength': 0.0,
                'direction': 'unknown'
            },
            'confidence': 0.0,
            'recommended_strategies': [],
            'data_sources': {
                'timeframes_analyzed': list(market_data.keys())
            }
        }

        try:
            if not market_data:
                raise ValueError("시장 데이터가 없습니다")

            # 각 시간프레임별 상태 분석 (1d 기본)
            primary_tf = '1d' if '1d' in market_data else list(market_data.keys())[0]
            primary_df = market_data[primary_tf]

            # 변동성 분석
            volatility_score, volatility_level = self.calculate_market_volatility(primary_df)
            result['volatility']['score'] = volatility_score
            result['volatility']['level'] = volatility_level

            # 추세 분석
            trend_strength, trend_direction = self.calculate_trend_strength(primary_df)
            result['trend']['strength'] = trend_strength
            result['trend']['direction'] = trend_direction

            # 시장 국면 감지
            market_phase = self.detect_market_phases(primary_df)
            result['market_phase'] = market_phase

            # 주요 시장 상황 판단
            primary_condition = 'unknown'
            secondary_condition = 'unknown'

            # 추세 방향 기반 조건
            if trend_direction in ['strongly_up', 'up']:
                primary_condition = 'trending_up'
            elif trend_direction in ['strongly_down', 'down']:
                primary_condition = 'trending_down'
            else:
                primary_condition = 'ranging'

            # 변동성 기반 보조 조건
            if volatility_level in ['high', 'very_high']:
                secondary_condition = 'high_volatility'
            elif volatility_level in ['low', 'very_low']:
                secondary_condition = 'low_volatility'

            # 시장 국면 기반 조건 조정
            if market_phase == 'accumulation':
                secondary_condition = 'accumulation'
            elif market_phase == 'distribution':
                secondary_condition = 'distribution'
            elif market_phase == 'consolidation':
                secondary_condition = 'consolidation'
            elif market_phase in ['euphoria', 'advancing'] and primary_condition == 'trending_up':
                secondary_condition = 'breakout'
            elif market_phase in ['capitulation', 'declining'] and primary_condition == 'trending_down':
                secondary_condition = 'breakdown'

            # 온체인 데이터 반영 (있는 경우)
            if onchain_data:
                # 온체인 신호 분석 (데이터 형식에 따라 조정 필요)
                onchain_signals = self.analyze_onchain_signals(onchain_data)
                result['data_sources']['onchain_signals'] = onchain_signals

                # 온체인 신호에 따른 조건 조정
                if onchain_signals.get('overall_sentiment') == 'bullish' and primary_condition != 'trending_down':
                    if primary_condition == 'ranging':
                        primary_condition = 'trending_up'
                    if secondary_condition == 'consolidation':
                        secondary_condition = 'accumulation'

                elif onchain_signals.get('overall_sentiment') == 'bearish' and primary_condition != 'trending_up':
                    if primary_condition == 'ranging':
                        primary_condition = 'trending_down'
                    if secondary_condition == 'consolidation':
                        secondary_condition = 'distribution'

            # 결과 구성
            result['primary_condition'] = primary_condition
            result['secondary_condition'] = secondary_condition

            # 추천 전략 선택
            recommended_strategies = []

            if primary_condition in self.condition_strategies:
                recommended_strategies.extend(self.condition_strategies[primary_condition])

            if secondary_condition in self.condition_strategies and secondary_condition != primary_condition:
                for strategy in self.condition_strategies[secondary_condition]:
                    if strategy not in recommended_strategies:
                        recommended_strategies.append(strategy)

            result['recommended_strategies'] = recommended_strategies[:3]  # 상위 3개만 선택

            # 분류 신뢰도 계산
            confidence_factors = []

            # 추세 강도에 따른 신뢰도
            if trend_strength > 2:
                confidence_factors.append(min(100, trend_strength * 10))
            else:
                confidence_factors.append(min(50, trend_strength * 25))

            # 변동성 수준에 따른 신뢰도
            if volatility_level in ['high', 'very_high', 'low', 'very_low']:
                confidence_factors.append(70)
            else:
                confidence_factors.append(50)

            # 각 시간프레임 정합성에 따른 신뢰도
            timeframe_consistency = self.check_timeframe_consistency(market_data)
            confidence_factors.append(timeframe_consistency * 100)

            # 평균 신뢰도 계산
            result['confidence'] = sum(confidence_factors) / len(confidence_factors)

            self.logger.info(
                f"시장 상황 분류 완료: {primary_condition}/{secondary_condition}, 국면: {market_phase}, 신뢰도: {result['confidence']:.2f}%")

        except Exception as e:
            self.logger.error(f"시장 상황 분류 중 오류 발생: {e}")
            result['error'] = str(e)

        return result

    def analyze_onchain_signals(
            self,
            onchain_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        온체인 데이터의 신호를 분석합니다.

        Args:
            onchain_data: 온체인 데이터

        Returns:
            Dict: 온체인 신호 분석 결과
        """
        result = {
            'bullish_signals': [],
            'bearish_signals': [],
            'neutral_signals': [],
            'overall_sentiment': 'neutral'
        }

        try:
            if not onchain_data or not onchain_data.get('data'):
                return result

            metrics_data = onchain_data.get('data', {})

            # 각 지표의 신호 카운팅
            bullish_count = 0
            bearish_count = 0
            neutral_count = 0

            # 가중치 설정 (중요 지표에 더 높은 가중치)
            weights = {
                "Exchange Reserve": 3,
                "Exchange Netflow": 3,
                "Adjusted SOPR": 2,
                "MVRV Ratio": 2,
                "Net Unrealized Profit/Loss (NUPL)": 2,
                "Exchange Whale Ratio": 1,
                "Coinbase Premium": 1
            }

            total_weight = 0
            weighted_score = 0

            for metric_name, metric_data in metrics_data.items():
                if isinstance(metric_data, dict) and 'signal' in metric_data:
                    signal = metric_data.get('signal', 'neutral')
                    metric_weight = weights.get(metric_name, 1)
                    total_weight += metric_weight

                    # 신호 분류 및 가중 점수 계산
                    if 'bullish' in signal:
                        result['bullish_signals'].append(metric_name)
                        bullish_count += 1

                        if 'very' in signal:
                            weighted_score += 2 * metric_weight
                        else:
                            weighted_score += 1 * metric_weight

                    elif 'bearish' in signal:
                        result['bearish_signals'].append(metric_name)
                        bearish_count += 1

                        if 'very' in signal:
                            weighted_score -= 2 * metric_weight
                        else:
                            weighted_score -= 1 * metric_weight

                    else:
                        result['neutral_signals'].append(metric_name)
                        neutral_count += 1

            # 전체 분위기 판단
            if total_weight > 0:
                normalized_score = weighted_score / total_weight

                if normalized_score > 0.5:
                    result['overall_sentiment'] = 'strongly_bullish'
                elif normalized_score > 0.2:
                    result['overall_sentiment'] = 'bullish'
                elif normalized_score < -0.5:
                    result['overall_sentiment'] = 'strongly_bearish'
                elif normalized_score < -0.2:
                    result['overall_sentiment'] = 'bearish'
                else:
                    result['overall_sentiment'] = 'neutral'

            # 간단한 통계 추가
            result['stats'] = {
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'neutral_count': neutral_count,
                'sentiment_score': weighted_score / total_weight if total_weight > 0 else 0
            }

        except Exception as e:
            self.logger.error(f"온체인 신호 분석 중 오류 발생: {e}")

        return result

    def check_timeframe_consistency(
            self,
            market_data: Dict[str, pd.DataFrame]
    ) -> float:
        """
        여러 시간프레임 간의 일관성을 검사합니다.

        Args:
            market_data: 시간프레임별 OHLCV 데이터

        Returns:
            float: 일관성 점수 (0-1)
        """
        if len(market_data) < 2:
            return 1.0  # 비교할 수 없으면 완전 일관성으로 간주

        # 각 시간프레임의 추세 방향 계산
        trend_directions = {}

        for tf, df in market_data.items():
            _, direction = self.calculate_trend_strength(df)
            trend_directions[tf] = direction

        # 추세 방향 그룹화
        direction_counts = {
            'up': sum(1 for d in trend_directions.values() if d in ['up', 'strongly_up']),
            'down': sum(1 for d in trend_directions.values() if d in ['down', 'strongly_down']),
            'neutral': sum(1 for d in trend_directions.values() if d == 'neutral')
        }

        # 최대 일치 방향의 비율 계산
        max_count = max(direction_counts.values())
        consistency_score = max_count / len(trend_directions)

        return consistency_score

    def save_classification_results(
            self,
            symbol: str,
            results: Dict[str, Any]
    ) -> None:
        """
        분류 결과를 저장합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            results: 분류 결과
        """
        timestamp_ms = results.get('timestamp_ms', int(time.time() * 1000))

        # Redis에 저장
        redis_key = f"analysis:market_condition:{symbol.replace('/', '_')}"
        self.redis_client.set(redis_key, json.dumps(results))
        self.redis_client.expire(redis_key, 86400)  # 1일 후 만료

        # PostgreSQL에 저장
        cursor = self.db_conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO market_conditions 
                (symbol, timestamp_ms, primary_condition, secondary_condition, market_phase,
                 volatility_level, trend_strength, recommended_strategies,
                 classification_confidence, data_sources)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp_ms) 
                DO UPDATE SET
                    primary_condition = EXCLUDED.primary_condition,
                    secondary_condition = EXCLUDED.secondary_condition,
                    market_phase = EXCLUDED.market_phase,
                    volatility_level = EXCLUDED.volatility_level,
                    trend_strength = EXCLUDED.trend_strength,
                    recommended_strategies = EXCLUDED.recommended_strategies,
                    classification_confidence = EXCLUDED.classification_confidence,
                    data_sources = EXCLUDED.data_sources,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    symbol,
                    timestamp_ms,
                    results.get('primary_condition', 'unknown'),
                    results.get('secondary_condition', 'unknown'),
                    results.get('market_phase', 'unknown'),
                    float(results.get('volatility', {}).get('score', 0)),  # 여기 수정
                    float(results.get('trend', {}).get('strength', 0)),  # 여기 수정
                    json.dumps(results.get('recommended_strategies', [])),
                    float(results.get('confidence', 0)),  # 여기 수정
                    json.dumps(results.get('data_sources', {}))
                )
            )

            self.db_conn.commit()
            self.logger.info(f"분류 결과 저장됨: {symbol}")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"분류 결과 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

    def run_classification(
            self,
            symbol: str = "BTC/USDT",
            timeframes: List[str] = ['1h', '4h', '1d', '1w']
    ) -> Dict[str, Any]:
        """
        시장 상황 분류를 실행합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            timeframes: 분석할 시간프레임 목록

        Returns:
            Dict: 분류 결과
        """
        self.logger.info(f"시장 상황 분류 시작: {symbol}, 시간프레임: {timeframes}")

        try:
            # 시장 데이터 가져오기
            market_data = self.fetch_multi_timeframe_data(symbol, timeframes)

            if not market_data:
                raise ValueError(f"심볼 {symbol}에 대한 시장 데이터를 가져올 수 없습니다")

            # 온체인 데이터 가져오기
            onchain_data = self.fetch_onchain_data(symbol)

            # 공포&탐욕 지수 가져오기
            fear_greed_data = self.fetch_fear_greed_data()

            # 시장 상황 분류
            classification_results = self.classify_market_condition(market_data, onchain_data)

            # 공포&탐욕 지수 추가
            if fear_greed_data:
                classification_results['data_sources']['fear_greed'] = fear_greed_data

                # 공포&탐욕 지수에 따른 추가 조정
                fear_greed_index = fear_greed_data.get('fear_greed_index', 50)

                if fear_greed_index <= 25:  # 극단적 공포
                    if classification_results['primary_condition'] == 'ranging':
                        classification_results['secondary_condition'] = 'accumulation'

                elif fear_greed_index >= 75:  # 극단적 탐욕
                    if classification_results['primary_condition'] == 'ranging':
                        classification_results['secondary_condition'] = 'distribution'

            # 결과 저장
            classification_results['symbol'] = symbol
            self.save_classification_results(symbol, classification_results)

            self.logger.info(f"시장 상황 분류 완료: {symbol}")
            return classification_results

        except Exception as e:
            self.logger.error(f"시장 상황 분류 중 오류 발생: {e}")
            return {
                'symbol': symbol,
                'timestamp_ms': int(time.time() * 1000),
                'error': str(e)
            }

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")

        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            self.logger.info("Redis 연결 종료됨")


# 직접 실행 시 분류기 시작
if __name__ == "__main__":
    classifier = MarketConditionClassifier()

    try:
        results = classifier.run_classification("BTC/USDT", ['1h', '4h', '1d', '1w'])

        print(f"===== 시장 상황 분류 결과 =====")
        print(f"주요 상황: {results['primary_condition']}")
        print(f"보조 상황: {results.get('secondary_condition', 'none')}")
        print(f"시장 국면: {results['market_phase']}")
        print(f"변동성: {results['volatility']['score']:.2f} ({results['volatility']['level']})")
        print(f"추세 강도: {results['trend']['strength']:.2f} ({results['trend']['direction']})")
        print(f"분류 신뢰도: {results['confidence']:.2f}%")

        print(f"\n추천 전략:")
        for strategy in results['recommended_strategies']:
            print(f"- {strategy}")

    except Exception as e:
        print(f"분류 오류: {e}")
    finally:
        classifier.close()
