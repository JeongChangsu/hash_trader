# execution/entry_exit_manager.py
"""
진입/퇴출 관리자

이 모듈은 전략 신호를 받아 최적의 진입 및 퇴출 시점을 결정합니다.
여러 전략에서 생성된 신호를 평가하고 조합하여 최종 매매 결정을 내립니다.
"""

import logging
import json
import redis
import psycopg2
import pandas as pd
import numpy as np
import time
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from config.settings import REDIS_CONFIG, POSTGRES_CONFIG, STRATEGY_WEIGHTS
from config.logging_config import configure_logging
from execution.risk_manager import RiskManager


class EntryExitManager:
    """
    진입/퇴출 시점을 관리하는 클래스
    """

    def __init__(self, symbol: str = "BTC/USDT"):
        """
        EntryExitManager 초기화

        Args:
            symbol: 거래 심볼
        """
        self.symbol = symbol
        self.logger = configure_logging("entry_exit_manager")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 기본 설정
        self.min_signal_strength = 60  # 최소 신호 강도
        self.min_confidence_score = 65  # 최소 신뢰도 점수
        self.min_risk_reward_ratio = 1.5  # 최소 위험 대비 보상 비율

        # 전략 가중치
        self.strategy_weights = STRATEGY_WEIGHTS

        # 활성 포지션 관리
        self.active_position = None

        # 캔들 마감 확인 캐시
        self.candle_closed_cache = {}

        self.logger.info(f"EntryExitManager 초기화됨, 심볼: {symbol}")

    def get_recent_signals(
            self,
            lookback_hours: int = 24
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        최근 전략 신호를 가져옵니다.

        Args:
            lookback_hours: 조회할 기간(시간)

        Returns:
            Dict: 전략별 신호 목록
        """
        self.logger.info(f"최근 {lookback_hours}시간 신호 조회 중...")

        try:
            # 시간 기준 설정
            cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
            cutoff_timestamp = int(cutoff_time.timestamp() * 1000)

            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    id, timestamp_ms, strategy_id, signal_type, direction,
                    strength, price, timeframe, message, entry_price,
                    take_profit_levels, stop_loss_level
                FROM 
                    strategy_signals
                WHERE 
                    symbol = %s AND timestamp_ms > %s
                ORDER BY 
                    timestamp_ms DESC
                """,
                (self.symbol, cutoff_timestamp)
            )

            rows = cursor.fetchall()

            # 전략별로 신호 분류
            strategy_signals = {}

            for row in cursor.fetchall():
                (
                    id, timestamp_ms, strategy_id, signal_type, direction,
                    strength, price, timeframe, message, entry_price,
                    take_profit_levels, stop_loss_level
                ) = row

                signal = {
                    "id": id,
                    "timestamp_ms": timestamp_ms,
                    "strategy_id": strategy_id,
                    "signal_type": signal_type,
                    "direction": direction,
                    "strength": strength,
                    "price": price,
                    "timeframe": timeframe,
                    "message": message,
                    "entry_price": entry_price,
                    "take_profit_levels": json.loads(take_profit_levels) if take_profit_levels else [],
                    "stop_loss_level": stop_loss_level
                }

                if strategy_id not in strategy_signals:
                    strategy_signals[strategy_id] = []

                strategy_signals[strategy_id].append(signal)

            self.logger.info(f"{len(rows)}개 신호 조회됨")
            return strategy_signals

        except Exception as e:
            self.logger.error(f"신호 조회 중 오류 발생: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

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
                    volatility_level, trend_strength, recommended_strategies,
                    classification_confidence
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
                    'recommended_strategies': json.loads(row[5]) if row[5] else [],
                    'confidence': row[6]
                }

            self.logger.warning(f"시장 상황 데이터가 없습니다: {self.symbol}")
            return {}

        except Exception as e:
            self.logger.error(f"시장 상황 조회 중 오류 발생: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

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
                return data.get('close', 0.0)

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
                return float(result[0])

            self.logger.warning(f"현재 가격 조회 실패: {self.symbol}")
            return 0.0

        except Exception as e:
            self.logger.error(f"현재 가격 조회 중 오류: {e}")
            return 0.0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def check_candle_closed(
            self,
            timeframe: str,
            timestamp_ms: int
    ) -> bool:
        """
        특정 시간프레임에서 캔들이 마감되었는지 확인합니다.

        Args:
            timeframe: 시간프레임 (예: '1h')
            timestamp_ms: 타임스탬프 (밀리초)

        Returns:
            bool: 캔들 마감 여부
        """
        # 캐시 키 생성
        cache_key = f"{timeframe}_{timestamp_ms}"

        # 캐시에 있으면 캐시된 결과 반환
        if cache_key in self.candle_closed_cache:
            return self.candle_closed_cache[cache_key]

        try:
            # 타임스탬프 변환
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

            # 시간프레임별 캔들 마감 시간 계산
            if timeframe == '1m':
                candle_end = timestamp + timedelta(minutes=1)
            elif timeframe == '5m':
                candle_end = timestamp + timedelta(minutes=5)
            elif timeframe == '15m':
                candle_end = timestamp + timedelta(minutes=15)
            elif timeframe == '1h':
                candle_end = timestamp + timedelta(hours=1)
            elif timeframe == '4h':
                candle_end = timestamp + timedelta(hours=4)
            elif timeframe == '1d':
                candle_end = timestamp + timedelta(days=1)
            elif timeframe == '1w':
                candle_end = timestamp + timedelta(weeks=1)
            else:
                # 지원되지 않는 시간프레임
                self.logger.warning(f"지원되지 않는 시간프레임: {timeframe}")
                return False

            # 현재 시간이 캔들 마감 시간을 지났는지 확인
            now = datetime.now()
            candle_closed = now > candle_end

            # 결과 캐싱
            self.candle_closed_cache[cache_key] = candle_closed

            return candle_closed

        except Exception as e:
            self.logger.error(f"캔들 마감 확인 중 오류: {e}")
            return False

    def evaluate_signal_compatibility(
            self,
            signal: Dict[str, Any],
            market_condition: Dict[str, Any]
    ) -> float:
        """
        신호와 시장 상황의 호환성을 평가합니다.

        Args:
            signal: 전략 신호
            market_condition: 시장 상황

        Returns:
            float: 호환성 점수 (0-1)
        """
        # 기본 호환성 점수
        compatibility = 0.5

        # 전략 가중치 적용
        strategy_id = signal.get('strategy_id', '')
        strategy_weight = self.strategy_weights.get(strategy_id, 0.25)

        # 시장 상황에 따른 추천 전략
        recommended_strategies = market_condition.get('recommended_strategies', [])

        # 추천 전략에 포함되어 있으면 호환성 증가
        if strategy_id in recommended_strategies:
            compatibility += 0.3

        # 시장 상태에 따른 호환성 평가
        primary_condition = market_condition.get('primary_condition', 'unknown')

        # 시장 상태와 전략 매칭
        if strategy_id == 'trend_following' and primary_condition in ['trending_up', 'trending_down']:
            compatibility += 0.2
        elif strategy_id == 'reversal' and primary_condition in ['overbought', 'oversold']:
            compatibility += 0.2
        elif strategy_id == 'breakout' and primary_condition in ['breakout', 'consolidation']:
            compatibility += 0.2
        elif strategy_id == 'range' and primary_condition in ['ranging', 'consolidation']:
            compatibility += 0.2

        # 신호 방향과 시장 추세 일치성 확인
        if primary_condition == 'trending_up' and signal.get('direction') == 'long':
            compatibility += 0.1
        elif primary_condition == 'trending_down' and signal.get('direction') == 'short':
            compatibility += 0.1

        # 호환성 점수 정규화 (0-1)
        compatibility = max(0.0, min(1.0, compatibility))

        return compatibility

    def rank_entry_signals(
            self,
            signals: List[Dict[str, Any]],
            market_condition: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        진입 신호를 랭킹합니다.

        Args:
            signals: 전략 신호 목록
            market_condition: 시장 상황

        Returns:
            List[Dict]: 랭킹된 신호 목록
        """
        if not signals:
            return []

        # 현재 가격
        current_price = self.get_current_price()

        # 각 신호에 점수 추가
        scored_signals = []

        for signal in signals:
            if signal.get('signal_type') != 'entry':
                continue

            # 신호 강도
            strength = signal.get('strength', 0)

            # 시장 상황과의 호환성
            compatibility = self.evaluate_signal_compatibility(signal, market_condition)

            # 위험 대비 보상 비율 (R:R)
            entry_price = signal.get('entry_price', current_price)
            stop_loss = signal.get('stop_loss_level', 0)

            if 'take_profit_levels' in signal and signal['take_profit_levels']:
                # 마지막 TP 레벨 (최대 목표)
                take_profit = signal['take_profit_levels'][-1]
            else:
                take_profit = 0

            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price) if take_profit else 0

            risk_reward_ratio = reward / risk if risk > 0 else 0

            # 캔들 마감 확인
            signal_time = signal.get('timestamp_ms', 0)
            timeframe = signal.get('timeframe', '1h')
            is_candle_closed = self.check_candle_closed(timeframe, signal_time)

            # 종합 점수 계산
            # (신호 강도 40% + 호환성 30% + R:R 30%) * 캔들 마감 보너스
            score = (0.4 * (strength / 100) +
                     0.3 * compatibility +
                     0.3 * min(1.0, risk_reward_ratio / 3.0))

            # 캔들 마감 보너스 (20% 추가)
            if is_candle_closed:
                score *= 1.2

            # 최종 점수 정규화 (0-100)
            final_score = min(100, score * 100)

            # 신호에 점수 추가
            scored_signal = signal.copy()
            scored_signal.update({
                'final_score': final_score,
                'compatibility': compatibility * 100,
                'risk_reward_ratio': risk_reward_ratio,
                'is_candle_closed': is_candle_closed
            })

            scored_signals.append(scored_signal)

        # 최종 점수 기준 내림차순 정렬
        ranked_signals = sorted(scored_signals, key=lambda x: x.get('final_score', 0), reverse=True)

        return ranked_signals

    def filter_exit_signals(
            self,
            active_position: Dict[str, Any],
            signals: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        활성 포지션에 대한 퇴출 신호를 필터링합니다.

        Args:
            active_position: 활성 포지션 정보
            signals: 전략 신호 목록

        Returns:
            List[Dict]: 필터링된 퇴출 신호 목록
        """
        if not active_position or not signals:
            return []

        # 현재 포지션 정보
        position_direction = active_position.get('direction', 'none')
        position_entry_price = active_position.get('entry_price', 0)

        # 현재 가격
        current_price = self.get_current_price()

        # 퇴출 신호 필터링
        exit_signals = []

        for signal in signals:
            # 진입 시점 이후의 신호만 고려
            if signal.get('timestamp_ms', 0) <= active_position.get('timestamp_ms', 0):
                continue

            signal_type = signal.get('signal_type', 'none')
            signal_direction = signal.get('direction', 'none')

            # 명시적 퇴출 신호
            is_exit_signal = signal_type == 'exit'

            # 반대 방향 진입 신호도 현재 포지션의 퇴출 신호로 간주
            is_reverse_signal = (signal_type == 'entry' and
                                 ((position_direction == 'long' and signal_direction == 'short') or
                                  (position_direction == 'short' and signal_direction == 'long')))

            if is_exit_signal or is_reverse_signal:
                # 신호에 점수 추가
                exit_signal = signal.copy()
                exit_signal.update({
                    'exit_type': 'signal',
                    'exit_reason': 'Explicit exit signal' if is_exit_signal else 'Reverse entry signal',
                    'exit_price': current_price,
                    'pnl_pct': self.calculate_pnl_percentage(position_direction, position_entry_price, current_price)
                })

                exit_signals.append(exit_signal)

        # 시간 기준 내림차순 정렬 (최신 신호 우선)
        sorted_exit_signals = sorted(exit_signals, key=lambda x: x.get('timestamp_ms', 0), reverse=True)

        return sorted_exit_signals

    def calculate_pnl_percentage(
            self,
            direction: str,
            entry_price: float,
            current_price: float
    ) -> float:
        """
        손익 비율을 계산합니다.

        Args:
            direction: 포지션 방향 ('long' 또는 'short')
            entry_price: 진입 가격
            current_price: 현재 가격

        Returns:
            float: 손익 비율 (%)
        """
        if direction == 'long':
            return (current_price - entry_price) / entry_price * 100
        elif direction == 'short':
            return (entry_price - current_price) / entry_price * 100
        else:
            return 0.0

    def check_take_profit_stop_loss(
            self,
            active_position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        활성 포지션의 TP/SL 도달 여부를 확인합니다.

        Args:
            active_position: 활성 포지션 정보

        Returns:
            Dict: TP/SL 확인 결과
        """
        result = {
            'hit_take_profit': False,
            'hit_stop_loss': False,
            'exit_price': 0,
            'exit_type': 'none',
            'tp_level_hit': None,
            'pnl_pct': 0
        }

        if not active_position:
            return result

        # 포지션 정보
        direction = active_position.get('direction', 'none')
        entry_price = active_position.get('entry_price', 0)
        take_profit_levels = active_position.get('take_profit_levels', [])
        stop_loss = active_position.get('stop_loss', 0)

        # 현재 가격
        current_price = self.get_current_price()

        if direction == 'long':
            # 롱 포지션 TP 확인
            for tp_level in take_profit_levels:
                if current_price >= tp_level:
                    result.update({
                        'hit_take_profit': True,
                        'exit_price': tp_level,
                        'exit_type': 'take_profit',
                        'tp_level_hit': tp_level,
                        'pnl_pct': self.calculate_pnl_percentage(direction, entry_price, tp_level)
                    })
                    break

            # 롱 포지션 SL 확인
            if stop_loss > 0 and current_price <= stop_loss:
                result.update({
                    'hit_stop_loss': True,
                    'exit_price': stop_loss,
                    'exit_type': 'stop_loss',
                    'pnl_pct': self.calculate_pnl_percentage(direction, entry_price, stop_loss)
                })

        elif direction == 'short':
            # 숏 포지션 TP 확인
            for tp_level in take_profit_levels:
                if current_price <= tp_level:
                    result.update({
                        'hit_take_profit': True,
                        'exit_price': tp_level,
                        'exit_type': 'take_profit',
                        'tp_level_hit': tp_level,
                        'pnl_pct': self.calculate_pnl_percentage(direction, entry_price, tp_level)
                    })
                    break

            # 숏 포지션 SL 확인
            if stop_loss > 0 and current_price >= stop_loss:
                result.update({
                    'hit_stop_loss': True,
                    'exit_price': stop_loss,
                    'exit_type': 'stop_loss',
                    'pnl_pct': self.calculate_pnl_percentage(direction, entry_price, stop_loss)
                })

        return result

    def determine_optimal_entry(
            self,
            signals: List[Dict[str, Any]],
            market_condition: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        최적의 진입 시점을 결정합니다.

        Args:
            signals: 전략 신호 목록
            market_condition: 시장 상황

        Returns:
            Optional[Dict]: 최적 진입 신호 또는 None
        """
        self.logger.info("최적 진입 시점 결정 중...")

        if not signals:
            self.logger.info("진입 신호 없음")
            return None

        # 진입 신호 랭킹
        ranked_signals = self.rank_entry_signals(signals, market_condition)

        if not ranked_signals:
            self.logger.info("유효한 진입 신호 없음")
            return None

        # 최고 점수 신호
        top_signal = ranked_signals[0]

        # 최소 기준 검증
        if (top_signal.get('final_score', 0) < self.min_signal_strength or
                top_signal.get('risk_reward_ratio', 0) < self.min_risk_reward_ratio):
            self.logger.info(
                f"최고 신호가 기준 미달: "
                f"점수={top_signal.get('final_score', 0):.2f}, "
                f"R:R={top_signal.get('risk_reward_ratio', 0):.2f}"
            )
            return None

        # 캔들 마감 확인 (필수)
        if not top_signal.get('is_candle_closed', False):
            self.logger.info(f"캔들 마감 대기 중: {top_signal.get('timeframe', '')}")
            return None

        self.logger.info(
            f"최적 진입 신호 발견: "
            f"{top_signal.get('direction', 'none')}, "
            f"전략={top_signal.get('strategy_id', '')}, "
            f"점수={top_signal.get('final_score', 0):.2f}, "
            f"R:R={top_signal.get('risk_reward_ratio', 0):.2f}"
        )

        return top_signal

    def manage_exits(
            self,
            active_position: Dict[str, Any],
            signals: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        퇴출 전략을 관리합니다.

        Args:
            active_position: 활성 포지션 정보
            signals: 전략 신호 목록

        Returns:
            Optional[Dict]: 퇴출 신호 또는 None
        """
        self.logger.info("퇴출 전략 관리 중...")

        if not active_position:
            self.logger.info("활성 포지션 없음")
            return None

        # TP/SL 확인
        tp_sl_check = self.check_take_profit_stop_loss(active_position)

        # TP 도달
        if tp_sl_check['hit_take_profit']:
            self.logger.info(
                f"이익 실현 레벨 도달: "
                f"가격={tp_sl_check['exit_price']}, "
                f"손익={tp_sl_check['pnl_pct']:.2f}%"
            )
            return {
                'exit_type': 'take_profit',
                'exit_price': tp_sl_check['exit_price'],
                'exit_reason': f"Take profit level hit: {tp_sl_check['tp_level_hit']}",
                'pnl_pct': tp_sl_check['pnl_pct']
            }

        # SL 도달
        if tp_sl_check['hit_stop_loss']:
            self.logger.info(
                f"손절 레벨 도달: "
                f"가격={tp_sl_check['exit_price']}, "
                f"손익={tp_sl_check['pnl_pct']:.2f}%"
            )
            return {
                'exit_type': 'stop_loss',
                'exit_price': tp_sl_check['exit_price'],
                'exit_reason': "Stop loss level hit",
                'pnl_pct': tp_sl_check['pnl_pct']
            }

        # 퇴출 신호 확인
        exit_signals = self.filter_exit_signals(active_position, signals)

        if exit_signals:
            exit_signal = exit_signals[0]  # 최신 퇴출 신호
            self.logger.info(
                f"퇴출 신호 발견: "
                f"유형={exit_signal.get('exit_type', 'unknown')}, "
                f"이유={exit_signal.get('exit_reason', 'unknown')}, "
                f"손익={exit_signal.get('pnl_pct', 0):.2f}%"
            )
            return exit_signal

        # 퇴출 없음
        return None

    def confirm_signal_after_candle_close(
            self,
            signal: Dict[str, Any]
    ) -> bool:
        """
        캔들 마감 후 신호를 확인합니다.

        Args:
            signal: 전략 신호

        Returns:
            bool: 신호 유효성
        """
        if not signal:
            return False

        # 신호 시간과 시간프레임
        signal_time = signal.get('timestamp_ms', 0)
        timeframe = signal.get('timeframe', '1h')

        # 캔들 마감 확인
        is_candle_closed = self.check_candle_closed(timeframe, signal_time)

        if is_candle_closed:
            self.logger.info(f"{timeframe} 캔들 마감 확인됨, 신호 유효")
            return True
        else:
            self.logger.info(f"{timeframe} 캔들 마감 대기 중, 신호 보류")
            return False

    def process_signals(self) -> Dict[str, Any]:
        """
        전략 신호를 처리하여 최종 매매 결정을 내립니다.

        Returns:
            Dict: 처리 결과
        """
        self.logger.info("신호 처리 시작...")

        result = {
            'action': 'none',
            'direction': 'none',
            'price': 0,
            'entry_signal': None,
            'exit_signal': None,
            'active_position': self.active_position,
            'timestamp': datetime.now().isoformat()
        }

        try:
            # 최근 신호 조회
            all_signals = []
            strategy_signals = self.get_recent_signals()

            for strategy_id, signals in strategy_signals.items():
                all_signals.extend(signals)

            # 시장 상황 조회
            market_condition = self.get_market_condition()

            # 활성 포지션이 있는 경우
            if self.active_position:
                # 퇴출 전략 관리
                exit_signal = self.manage_exits(self.active_position, all_signals)

                if exit_signal:
                    # 퇴출 결정
                    result.update({
                        'action': 'exit',
                        'direction': self.active_position.get('direction', 'none'),
                        'price': exit_signal.get('exit_price', self.get_current_price()),
                        'exit_signal': exit_signal,
                        'exit_reason': exit_signal.get('exit_reason', 'Unknown'),
                        'pnl_pct': exit_signal.get('pnl_pct', 0)
                    })

                    self.logger.info(
                        f"포지션 퇴출 결정: "
                        f"{result['direction']}, "
                        f"가격={result['price']}, "
                        f"손익={result.get('pnl_pct', 0):.2f}%"
                    )

                    # 활성 포지션 초기화
                    self.active_position = None

            # 활성 포지션이 없는 경우
            else:
                # 최적 진입 결정
                entry_signal = self.determine_optimal_entry(all_signals, market_condition)

                if entry_signal:
                    # 캔들 마감 확인
                    if self.confirm_signal_after_candle_close(entry_signal):
                        # 리스크 관리자 인스턴스 생성 (실제로는 더 효율적인 방법으로 관리)
                        risk_manager = RiskManager(self.symbol)

                        # 리스크 관리자를 통해 최종 TP/SL 결정
                        tp_sl_result = risk_manager.finalize_tp_sl(entry_signal, market_condition)

                        # 진입 결정
                        result.update({
                            'action': 'entry',
                            'direction': entry_signal.get('direction', 'none'),
                            'price': entry_signal.get('entry_price', self.get_current_price()),
                            'entry_signal': entry_signal,
                            'stop_loss': tp_sl_result['stop_loss'],
                            'take_profit_levels': tp_sl_result['take_profit_levels']
                        })

                        # 활성 포지션 설정
                        self.active_position = {
                            'direction': entry_signal.get('direction', 'none'),
                            'entry_price': result['price'],
                            'stop_loss': tp_sl_result['stop_loss'],
                            'take_profit_levels': tp_sl_result['take_profit_levels'],
                            'timestamp_ms': int(time.time() * 1000),
                            'strategy_id': entry_signal.get('strategy_id', 'unknown'),
                            'timeframe': entry_signal.get('timeframe', 'unknown')
                        }

                        self.logger.info(
                            f"포지션 진입 결정: "
                            f"{result['direction']}, "
                            f"가격={result['price']}, "
                            f"SL={result['stop_loss']}, "
                            f"TP 조정 이유={tp_sl_result['adjusted_reason']}"
                        )

        except Exception as e:
            self.logger.error(f"신호 처리 중 오류 발생: {e}")

        return result

    def save_trade_decision(self, decision: Dict[str, Any]) -> int:
        """
        매매 결정을 저장합니다.

        Args:
            decision: 매매 결정 데이터

        Returns:
            int: 결정 ID
        """
        try:
            timestamp_ms = int(time.time() * 1000)

            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO trade_decisions
                (timestamp_ms, symbol, action, direction, price, entry_signal_id, exit_signal_id,
                 stop_loss, take_profit_levels, exit_reason, pnl_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    timestamp_ms,
                    self.symbol,
                    decision.get('action', 'none'),
                    decision.get('direction', 'none'),
                    decision.get('price', 0),
                    decision.get('entry_signal', {}).get('id') if decision.get('entry_signal') else None,
                    decision.get('exit_signal', {}).get('id') if decision.get('exit_signal') else None,
                    decision.get('stop_loss', 0),
                    json.dumps(decision.get('take_profit_levels', [])),
                    decision.get('exit_reason', ''),
                    decision.get('pnl_pct', 0)
                )
            )

            decision_id = cursor.fetchone()[0]
            self.db_conn.commit()

            self.logger.info(f"매매 결정 저장됨: {decision_id}")
            return decision_id

        except Exception as e:
            self.logger.error(f"매매 결정 저장 중 오류 발생: {e}")
            if 'cursor' in locals() and 'db_conn' in locals():
                self.db_conn.rollback()
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def run(self) -> Dict[str, Any]:
        """
        진입/퇴출 관리자를 실행합니다.

        Returns:
            Dict: 실행 결과
        """
        self.logger.info("진입/퇴출 관리자 실행 중...")

        try:
            # 신호 처리
            decision = self.process_signals()

            # 액션이 있으면 저장
            if decision.get('action') != 'none':
                decision_id = self.save_trade_decision(decision)
                decision['decision_id'] = decision_id

            return decision

        except Exception as e:
            self.logger.error(f"진입/퇴출 관리자 실행 중 오류 발생: {e}")
            return {
                'action': 'none',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

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


# 직접 실행 시 매니저 시작
if __name__ == "__main__":
    manager = EntryExitManager("BTC/USDT")

    try:
        result = manager.run()
        print(f"실행 결과: 액션={result['action']}, 방향={result['direction']}")

        if result['action'] == 'entry':
            print(f"진입 가격: {result['price']}")
            print(f"손절가: {result['stop_loss']}")
            print(f"이익실현 레벨: {result['take_profit_levels']}")
        elif result['action'] == 'exit':
            print(f"퇴출 가격: {result['price']}")
            print(f"퇴출 이유: {result['exit_reason']}")
            print(f"손익: {result.get('pnl_pct', 0):.2f}%")

    except Exception as e:
        print(f"실행 오류: {e}")
    finally:
        manager.close()
