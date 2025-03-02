# strategies/base_strategy.py
"""
기본 전략 클래스

이 모듈은 모든 거래 전략의 기본 클래스를 제공합니다.
각 구체적인 전략은 이 기본 클래스를 상속하여 구현됩니다.
"""

import logging
import json
import pandas as pd
import numpy as np
import redis
import psycopg2
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from config.settings import REDIS_CONFIG, POSTGRES_CONFIG
from config.logging_config import configure_logging


class BaseStrategy(ABC):
    """
    모든 거래 전략의 기본 클래스
    """

    def __init__(
            self,
            strategy_id: str,
            strategy_name: str,
            symbol: str = "BTC/USDT"
    ):
        """
        BaseStrategy 초기화

        Args:
            strategy_id: 전략 고유 ID
            strategy_name: 전략 이름
            symbol: 거래 심볼
        """
        self.strategy_id = strategy_id
        self.strategy_name = strategy_name
        self.symbol = symbol

        # 로깅 설정
        self.logger = configure_logging(f"strategy_{strategy_id}")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 전략 상태
        self.active = False
        self.position_open = False
        self.position_side = None  # 'long' 또는 'short'
        self.position_size = 0.0
        self.entry_price = 0.0
        self.current_price = 0.0

        # 손익 설정
        self.take_profit_levels = []  # [(가격, 비율), ...]
        self.stop_loss_level = 0.0

        # 시장 데이터 캐시
        self.market_data = {}
        self.analysis_cache = {}

        self.logger.info(f"{strategy_name} 전략 초기화됨 (ID: {strategy_id})")

    def get_current_price(self) -> float:
        """
        현재 가격을 가져옵니다.

        Returns:
            float: 현재 가격
        """
        try:
            # Redis에서 최근 가격 확인
            redis_key = f"data:market:ohlcv:{self.symbol.replace('/', '_')}:1m"
            latest_data = self.redis_client.get(redis_key)

            if latest_data:
                data = json.loads(latest_data)
                self.current_price = data.get('close', 0.0)
                return self.current_price

            # Redis에 없으면 DB에서 조회
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    close 
                FROM 
                    market_ohlcv 
                WHERE 
                    symbol = %s AND timeframe = '1m'
                ORDER BY 
                    timestamp_ms DESC 
                LIMIT 1
                """,
                (self.symbol,)
            )

            result = cursor.fetchone()
            if result:
                self.current_price = float(result[0])
                return self.current_price

            self.logger.warning(f"현재 가격 조회 실패: {self.symbol}")
            return 0.0

        except Exception as e:
            self.logger.error(f"현재 가격 조회 중 오류: {e}")
            return 0.0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_ohlcv_data(self, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """
        OHLCV 데이터를 가져옵니다.

        Args:
            timeframe: 시간프레임 (예: '1h')
            limit: 캔들 수

        Returns:
            pd.DataFrame: OHLCV 데이터프레임
        """
        # 캐시 확인
        cache_key = f"{self.symbol}_{timeframe}_{limit}"
        if cache_key in self.market_data:
            return self.market_data[cache_key]

        try:
            cursor = self.db_conn.cursor()

            # 심볼을 데이터베이스 형식에 맞게 변환
            db_symbol = self.symbol

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
                self.logger.warning(f"OHLCV 데이터가 없습니다: {self.symbol} ({timeframe})")
                return pd.DataFrame()

            # 데이터프레임 생성 및 정렬
            df = pd.DataFrame(
                rows,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

            # 타임스탬프를 datetime으로 변환 및 오름차순 정렬
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp')

            # 테크니컬 지표 계산
            self.calculate_indicators(df)

            # 캐시에 저장
            self.market_data[cache_key] = df

            self.logger.info(f"OHLCV 데이터 로드됨: {self.symbol} ({timeframe}), {len(df)} 캔들")
            return df

        except Exception as e:
            self.logger.error(f"OHLCV 데이터 가져오기 실패: {e}")
            return pd.DataFrame()
        finally:
            if 'cursor' in locals():
                cursor.close()

    def calculate_indicators(self, df: pd.DataFrame) -> None:
        """
        기술적 지표를 계산합니다.

        Args:
            df: OHLCV 데이터프레임
        """
        if df.empty:
            return

        # 기본 기술적 지표 계산

        # 이동평균
        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema55'] = df['close'].ewm(span=55, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

        df['sma20'] = df['close'].rolling(window=20).mean()
        df['sma50'] = df['close'].rolling(window=50).mean()
        df['sma200'] = df['close'].rolling(window=200).mean()

        # 볼린저 밴드
        df['middle_band'] = df['close'].rolling(window=20).mean()
        df['std_dev'] = df['close'].rolling(window=20).std()
        df['upper_band'] = df['middle_band'] + 2 * df['std_dev']
        df['lower_band'] = df['middle_band'] - 2 * df['std_dev']

        # MACD
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd_line'] = df['ema12'] - df['ema26']
        df['macd_signal'] = df['macd_line'].ewm(span=9, adjust=False).mean()
        df['macd_histogram'] = df['macd_line'] - df['macd_signal']

        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # 스토캐스틱 오실레이터
        high_14 = df['high'].rolling(window=14).max()
        low_14 = df['low'].rolling(window=14).min()
        df['stoch_k'] = 100 * ((df['close'] - low_14) / (high_14 - low_14))
        df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()

        # ATR
        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - df['close'].shift(1))
        tr3 = abs(df['low'] - df['close'].shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=14).mean()
        df['atr_percent'] = df['atr'] / df['close'] * 100

    def get_market_condition(self) -> Dict[str, Any]:
        """
        현재 시장 상황을 가져옵니다.

        Returns:
            Dict: 시장 상황 데이터
        """
        try:
            # Redis에서 최신 시장 상황 확인
            redis_key = f"analysis:market_condition:{self.symbol.replace('/', '_')}"
            cached_data = self.redis_client.get(redis_key)

            if cached_data:
                return json.loads(cached_data)

            # Redis에 없으면 DB에서 조회
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    primary_condition, secondary_condition, market_phase,
                    volatility_level, trend_strength, data_sources
                FROM 
                    market_conditions
                WHERE 
                    symbol = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """,
                (self.symbol,)
            )

            row = cursor.fetchone()
            if row:
                return {
                    'primary_condition': row[0],
                    'secondary_condition': row[1],
                    'market_phase': row[2],
                    'volatility': {'level': row[3]},
                    'trend': {'strength': row[4]},
                    'data_sources': json.loads(row[5]) if row[5] else {}
                }

            self.logger.warning(f"시장 상황 데이터가 없습니다: {self.symbol}")
            return {}

        except Exception as e:
            self.logger.error(f"시장 상황 조회 중 오류: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_multi_timeframe_analysis(self) -> Dict[str, Any]:
        """
        다중 시간프레임 분석 결과를 가져옵니다.

        Returns:
            Dict: 다중 시간프레임 분석 결과
        """
        try:
            # Redis에서 최신 분석 결과 확인
            redis_key = f"analysis:multi_tf:{self.symbol.replace('/', '_')}"
            cached_data = self.redis_client.get(redis_key)

            if cached_data:
                return json.loads(cached_data)

            # Redis에 없으면 DB에서 조회
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    tf_coherence, dominant_trend, htf_signals, ltf_signals, recommendations
                FROM 
                    multi_timeframe_analysis
                WHERE 
                    symbol = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """,
                (self.symbol,)
            )

            row = cursor.fetchone()
            if row:
                return {
                    'timeframe_coherence': row[0],
                    'dominant_trend': row[1],
                    'htf_signals': json.loads(row[2]) if row[2] else {},
                    'ltf_signals': json.loads(row[3]) if row[3] else {},
                    'recommendations': json.loads(row[4]) if row[4] else {}
                }

            self.logger.warning(f"다중 시간프레임 분석 결과가 없습니다: {self.symbol}")
            return {}

        except Exception as e:
            self.logger.error(f"다중 시간프레임 분석 조회 중 오류: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_support_resistance_levels(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        지지/저항 레벨을 가져옵니다.

        Returns:
            Dict: 지지/저항 레벨
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    level_type, price_level, strength, touch_count, timeframe
                FROM 
                    support_resistance_levels
                WHERE 
                    symbol = %s
                    AND created_at > NOW() - INTERVAL '1 day'
                ORDER BY 
                    level_type, strength DESC
                """,
                (self.symbol,)
            )

            rows = cursor.fetchall()

            support_levels = []
            resistance_levels = []

            for row in rows:
                level_type, price, strength, touch_count, timeframe = row
                level_data = {
                    'price': float(price),
                    'strength': float(strength),
                    'touch_count': int(touch_count),
                    'timeframe': timeframe
                }

                if level_type == 'support':
                    support_levels.append(level_data)
                elif level_type == 'resistance':
                    resistance_levels.append(level_data)

            return {
                'support': support_levels,
                'resistance': resistance_levels
            }

        except Exception as e:
            self.logger.error(f"지지/저항 레벨 조회 중 오류: {e}")
            return {'support': [], 'resistance': []}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_liquidation_clusters(self) -> Dict[str, Any]:
        """
        청산 클러스터 데이터를 가져옵니다.

        Returns:
            Dict: 청산 클러스터 데이터
        """
        try:
            # Redis에서 최신 데이터 확인
            redis_key = "data:liquidation_heatmap:latest"
            cached_data = self.redis_client.get(redis_key)

            if cached_data:
                return json.loads(cached_data)

            # Redis에 없으면 DB에서 조회
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    data
                FROM 
                    liquidation_heatmap
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])

            self.logger.warning(f"청산 클러스터 데이터가 없습니다")
            return {}

        except Exception as e:
            self.logger.error(f"청산 클러스터 조회 중 오류: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_optimal_strategy_recommendation(self) -> Dict[str, Any]:
        """
        최적 전략 추천을 가져옵니다.

        Returns:
            Dict: 전략 추천 데이터
        """
        try:
            # Redis에서 최신 추천 확인
            redis_key = f"strategy:recommendation:{self.symbol.replace('/', '_')}"
            cached_data = self.redis_client.get(redis_key)

            if cached_data:
                return json.loads(cached_data)

            # Redis에 없으면 DB에서 조회
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    primary_strategy, secondary_strategy, strategies_ranking,
                    confidence, entry_points, tp_sl_recommendations
                FROM 
                    ai_strategy_recommendations
                WHERE 
                    symbol = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """,
                (self.symbol,)
            )

            row = cursor.fetchone()
            if row:
                return {
                    'primary_strategy': row[0],
                    'secondary_strategy': row[1],
                    'ranking': json.loads(row[2]) if row[2] else [],
                    'confidence': row[3],
                    'entry_criteria': json.loads(row[4]) if row[4] else [],
                    'tp_sl': json.loads(row[5]) if row[5] else {}
                }

            self.logger.warning(f"전략 추천 데이터가 없습니다: {self.symbol}")
            return {}

        except Exception as e:
            self.logger.error(f"전략 추천 조회 중 오류: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def calculate_optimal_position_size(
            self,
            account_balance: float,
            risk_per_trade: float,
            stop_loss_pct: float,
            max_trade_size: Optional[float] = None
    ) -> float:
        """
        최적 포지션 크기를 계산합니다.

        Args:
            account_balance: 계정 잔고
            risk_per_trade: 거래당 위험 비율 (0-1)
            stop_loss_pct: 손절 비율 (0-1)
            max_trade_size: 최대 거래 크기 (선택적)

        Returns:
            float: 포지션 크기
        """
        if stop_loss_pct <= 0:
            self.logger.warning("손절 비율이 0 이하입니다. 기본값 0.01 사용")
            stop_loss_pct = 0.01

        # 위험 금액 계산
        risk_amount = account_balance * risk_per_trade

        # 포지션 크기 계산
        position_size = risk_amount / stop_loss_pct

        # 최대 거래 크기 제한
        if max_trade_size and position_size > max_trade_size:
            position_size = max_trade_size

        self.logger.info(
            f"포지션 크기 계산: ${position_size:.2f} (계정 잔고: ${account_balance:.2f}, 위험: {risk_per_trade:.2%}, 손절: {stop_loss_pct:.2%})")

        return position_size

    def calculate_optimal_leverage(
            self,
            market_volatility: float,
            max_leverage: int = 10
    ) -> int:
        """
        최적 레버리지를 계산합니다.

        Args:
            market_volatility: 시장 변동성 (%)
            max_leverage: 최대 레버리지

        Returns:
            int: 레버리지
        """
        if market_volatility <= 0:
            self.logger.warning("시장 변동성이 0 이하입니다. 기본값 3% 사용")
            market_volatility = 3.0

        # 기본 레버리지 계산 (변동성이 클수록 낮은 레버리지)
        base_leverage = int(15 / market_volatility)

        # 범위 제한
        leverage = max(1, min(base_leverage, max_leverage))

        self.logger.info(f"레버리지 계산: {leverage}x (변동성: {market_volatility:.2f}%, 최대 레버리지: {max_leverage}x)")

        return leverage

    def save_signal(
            self,
            signal_type: str,
            direction: str,
            strength: float,
            price: float,
            timeframe: str,
            message: str,
            entry_price: Optional[float] = None,
            take_profit_levels: Optional[List[float]] = None,
            stop_loss_level: Optional[float] = None
    ) -> int:
        """
        트레이딩 신호를 저장합니다.

        Args:
            signal_type: 신호 유형 (예: 'entry', 'exit')
            direction: 방향 (예: 'long', 'short')
            strength: 신호 강도 (0-100)
            price: 신호 가격
            timeframe: 신호 시간프레임
            message: 신호 메시지
            entry_price: 진입 가격 (선택적)
            take_profit_levels: 이익 실현 레벨 목록 (선택적)
            stop_loss_level: 손절 레벨 (선택적)

        Returns:
            int: 신호 ID
        """
        try:
            timestamp_ms = int(datetime.now().timestamp() * 1000)

            # 신호 데이터
            signal_data = {
                'timestamp_ms': timestamp_ms,
                'symbol': self.symbol,
                'strategy_id': self.strategy_id,
                'strategy_name': self.strategy_name,
                'signal_type': signal_type,
                'direction': direction,
                'strength': strength,
                'price': price,
                'timeframe': timeframe,
                'message': message,
                'entry_price': entry_price,
                'take_profit_levels': take_profit_levels,
                'stop_loss_level': stop_loss_level
            }

            # Redis에 저장
            redis_key = f"signal:{self.strategy_id}:{timestamp_ms}"
            self.redis_client.set(redis_key, json.dumps(signal_data))

            # Redis 스트림에 추가
            stream_key = f"stream:signals:{self.symbol.replace('/', '_')}"
            self.redis_client.xadd(stream_key, {'data': json.dumps(signal_data)})

            # PostgreSQL에 저장
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO strategy_signals
                (timestamp_ms, symbol, strategy_id, strategy_name, signal_type, direction,
                strength, price, timeframe, message, entry_price, take_profit_levels, stop_loss_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    timestamp_ms,
                    self.symbol,
                    self.strategy_id,
                    self.strategy_name,
                    signal_type,
                    direction,
                    strength,
                    price,
                    timeframe,
                    message,
                    entry_price,
                    json.dumps(take_profit_levels) if take_profit_levels else None,
                    stop_loss_level
                )
            )

            signal_id = cursor.fetchone()[0]
            self.db_conn.commit()

            self.logger.info(f"트레이딩 신호 저장됨: {signal_type}, {direction}, ID: {signal_id}")

            return signal_id

        except Exception as e:
            self.logger.error(f"트레이딩 신호 저장 중 오류: {e}")
            if 'cursor' in locals() and 'db_conn' in locals():
                self.db_conn.rollback()
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()

    @abstractmethod
    def calculate_signal(self) -> Dict[str, Any]:
        """
        전략별 트레이딩 신호를 계산합니다.
        이 메서드는 각 구체적인 전략 클래스에서 구현해야 합니다.

        Returns:
            Dict: 신호 데이터
        """
        pass

    def activate(self) -> bool:
        """
        전략을 활성화합니다.

        Returns:
            bool: 활성화 성공 여부
        """
        self.active = True
        self.logger.info(f"{self.strategy_name} 전략 활성화됨")
        return True

    def deactivate(self) -> bool:
        """
        전략을 비활성화합니다.

        Returns:
            bool: 비활성화 성공 여부
        """
        self.active = False
        self.logger.info(f"{self.strategy_name} 전략 비활성화됨")
        return True

    def is_active(self) -> bool:
        """
        전략이 활성화되어 있는지 확인합니다.

        Returns:
            bool: 활성화 여부
        """
        return self.active

    def clear_cache(self) -> None:
        """
        캐시를 초기화합니다.
        """
        self.market_data.clear()
        self.analysis_cache.clear()
        self.logger.info("캐시 초기화됨")

    def close(self) -> None:
        """
        자원을 정리합니다.
        """
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")

        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            self.logger.info("Redis 연결 종료됨")
