# execution/risk_manager.py
"""
위험 관리자

이 모듈은 트레이딩 시스템의 위험 관리를 담당합니다.
포지션 크기, 레버리지, 손절 및 이익실현 등을 결정하며
자본 보존과 장기적인 수익성을 최적화합니다.
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

from config.settings import REDIS_CONFIG, POSTGRES_CONFIG, DEFAULT_RISK_PER_TRADE, MAX_LEVERAGE
from config.logging_config import configure_logging


class RiskManager:
    """
    트레이딩 위험을 관리하고 자본 보존 규칙을 적용하는 클래스
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            account_balance: float = 10000.0,
            config: Optional[Dict[str, Any]] = None
    ):
        """
        RiskManager 초기화

        Args:
            symbol: 거래 심볼
            account_balance: 계정 잔고
            config: 위험 관리 설정
        """
        self.symbol = symbol
        self.account_balance = account_balance
        self.logger = configure_logging("risk_manager")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 기본 위험 관리 설정
        self.default_config = {
            "risk_per_trade": DEFAULT_RISK_PER_TRADE,  # 거래당 계정 자본의 %
            "max_risk_per_day": 0.05,  # 일일 최대 위험 (계정의 %)
            "max_leverage": MAX_LEVERAGE,  # 최대 레버리지
            "max_positions": 1,  # 동시 포지션 최대 개수
            "min_risk_reward_ratio": 1.5,  # 최소 위험 대비 보상 비율
            "sl_atr_multiplier": 1.5,  # 손절 ATR 배수
            "tp_atr_multiplier": {
                "first": 1.0,  # 첫 번째 TP 목표
                "second": 2.0,  # 두 번째 TP 목표
                "third": 3.0,  # 세 번째 TP 목표
            },
            "sl_buffer_percent": 0.5,  # 손절 추가 버퍼 (%)
            "partial_tp_percentages": [0.3, 0.3, 0.4],  # 부분 TP 비율
            "reduce_risk_after_loss": True,  # 손실 후 위험 감소
            "loss_streak_reduction": 0.5,  # 연속 손실 시 위험 감소 비율
            "increase_risk_after_win": False,  # 수익 후 위험 증가
            "win_streak_increase": 0.1,  # 연속 수익 시 위험 증가 비율
            "daily_loss_limit": 0.03,  # 일일 손실 한도 (계정의 %)
            "weekly_loss_limit": 0.07,  # 주간 손실 한도 (계정의 %)
            "monthly_loss_limit": 0.15,  # 월간 손실 한도 (계정의 %)
            "drawdown_reduction_threshold": 0.1,  # 자본 감소 시 위험 조정 임계값
            "drawdown_reduction_factor": 0.5,  # 자본 감소 시 위험 조정 계수
            "volatility_based_sizing": True,  # 변동성 기반 포지션 크기 조정
            "volatility_sizing_factor": 0.8,  # 고변동성 시 포지션 크기 감소 계수
            "trend_confirm_sizing": True,  # 추세 확인에 따른 포지션 크기 조정
            "trend_confirm_factor": 1.2,  # 추세 확인 시 포지션 크기 증가 계수
            "htf_confirm_factor": 1.2,  # HTF 확인 시 포지션 크기 증가 계수
            "conflicting_signals_reduction": 0.7,  # 상충 신호 시 포지션 크기 감소 계수
            "liquidation_target_percentage": 0.8,  # 청산 가격까지 남은 버퍼 (%)
            "min_position_size_usd": 100,  # 최소 포지션 크기 (USD)
            "max_position_size_usd": 5000,  # 최대 포지션 크기 (USD)
            "custom_risk_by_strategy": {  # 전략별 맞춤 위험 비율
                "trend_following": 1.0,  # 기본 위험의 100%
                "breakout": 0.9,  # 기본 위험의 90%
                "reversal": 0.8,  # 기본 위험의 80%
                "range": 0.7,  # 기본 위험의 70%
            }
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # 거래 실적 캐시
        self.trade_history = []
        self.current_winning_streak = 0
        self.current_losing_streak = 0
        self.daily_pnl = 0.0
        self.weekly_pnl = 0.0
        self.monthly_pnl = 0.0
        self.max_account_balance = account_balance
        self.current_drawdown = 0.0

        # 활성 포지션 추적
        self.active_positions = []

        self.logger.info(f"RiskManager 초기화됨: {symbol}, 자본: ${account_balance:.2f}")

    def calculate_position_size(
            self,
            entry_price: float,
            stop_loss: float,
            direction: str,
            strategy: str = "trend_following",
            signal_strength: float = 70.0,
            market_condition: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        최적의 포지션 크기와 레버리지를 계산합니다.

        Args:
            entry_price: 진입 가격
            stop_loss: 손절 가격
            direction: 포지션 방향 ('long' 또는 'short')
            strategy: 사용 중인 전략
            signal_strength: 신호 강도 (0-100)
            market_condition: 시장 상황 데이터

        Returns:
            Dict: 포지션 크기, 레버리지, 위험 정보
        """
        self.logger.info(f"포지션 크기 계산: {direction}, 진입: {entry_price}, 손절: {stop_loss}")

        # 기본 결과 구조
        result = {
            "position_size_usd": 0.0,
            "position_size_coins": 0.0,
            "leverage": 1.0,
            "risk_amount_usd": 0.0,
            "liquidation_price": 0.0,
            "max_leverage": self.config["max_leverage"],
            "risk_percent": self.config["risk_per_trade"] * 100,
            "adjusted_factors": {}
        }

        try:
            # 손절 거리 계산
            if direction == "long":
                stop_distance = entry_price - stop_loss
                stop_distance_percent = (stop_distance / entry_price) * 100
            else:  # short
                stop_distance = stop_loss - entry_price
                stop_distance_percent = (stop_distance / entry_price) * 100

            if stop_distance <= 0:
                self.logger.error("손절 거리가 0 이하입니다")
                return result

            # 1. 기본 위험 계산 (계정의 %)
            base_risk_percent = self.config["risk_per_trade"]

            # 전략별 위험 조정
            strategy_risk_factor = self.config["custom_risk_by_strategy"].get(strategy, 1.0)
            adjusted_risk_percent = base_risk_percent * strategy_risk_factor
            result["adjusted_factors"]["strategy"] = strategy_risk_factor

            # 2. 연속 손익에 따른 위험 조정
            streak_factor = 1.0
            if self.config["reduce_risk_after_loss"] and self.current_losing_streak > 0:
                streak_factor = max(0.5, 1.0 - (self.current_losing_streak * self.config["loss_streak_reduction"]))
            elif self.config["increase_risk_after_win"] and self.current_winning_streak > 0:
                streak_factor = min(1.5, 1.0 + (self.current_winning_streak * self.config["win_streak_increase"]))

            adjusted_risk_percent *= streak_factor
            result["adjusted_factors"]["streak"] = streak_factor

            # 3. 자본 감소(drawdown)에 따른 위험 조정
            drawdown_factor = 1.0
            if self.current_drawdown > self.config["drawdown_reduction_threshold"]:
                drawdown_factor = max(0.5, 1.0 - self.config["drawdown_reduction_factor"])

            adjusted_risk_percent *= drawdown_factor
            result["adjusted_factors"]["drawdown"] = drawdown_factor

            # 4. 시장 상황에 따른 위험 조정
            market_factor = 1.0
            if market_condition:
                # 변동성 기반 조정
                if self.config["volatility_based_sizing"]:
                    volatility_level = market_condition.get("volatility", {}).get("level", "medium")
                    if volatility_level in ["high", "very_high"]:
                        market_factor *= self.config["volatility_sizing_factor"]

                # 추세 확인 기반 조정
                if self.config["trend_confirm_sizing"]:
                    trend_direction = market_condition.get("trend", {}).get("direction", "neutral")
                    trend_strength = market_condition.get("trend", {}).get("strength", 0)

                    if ((direction == "long" and trend_direction in ["up", "strongly_up"]) or
                            (direction == "short" and trend_direction in ["down", "strongly_down"])):
                        market_factor *= self.config["trend_confirm_factor"]

            adjusted_risk_percent *= market_factor
            result["adjusted_factors"]["market"] = market_factor

            # 5. 신호 강도에 따른 위험 조정
            signal_factor = 1.0
            if signal_strength > 80:
                signal_factor = 1.2
            elif signal_strength < 60:
                signal_factor = 0.8

            adjusted_risk_percent *= signal_factor
            result["adjusted_factors"]["signal"] = signal_factor

            # 6. 일일/주간 손익에 따른 위험 조정
            limit_factor = 1.0
            if self.daily_pnl < -self.config["daily_loss_limit"] * self.account_balance:
                limit_factor = 0.5  # 일일 손실 한도 초과 시 위험 절반으로 감소
            elif self.weekly_pnl < -self.config["weekly_loss_limit"] * self.account_balance:
                limit_factor = 0.7  # 주간 손실 한도 초과 시 위험 30% 감소

            adjusted_risk_percent *= limit_factor
            result["adjusted_factors"]["limit"] = limit_factor

            # 최종 위험 비율 (0.1% ~ 3% 범위 제한)
            final_risk_percent = max(0.001, min(0.03, adjusted_risk_percent))

            # 위험 금액 계산
            risk_amount_usd = self.account_balance * final_risk_percent
            result["risk_amount_usd"] = risk_amount_usd
            result["risk_percent"] = final_risk_percent * 100

            # 7. 포지션 크기 계산
            position_size_usd = risk_amount_usd / (stop_distance_percent / 100)

            # 포지션 크기 한도 적용
            position_size_usd = max(self.config["min_position_size_usd"],
                                    min(self.config["max_position_size_usd"], position_size_usd))

            result["position_size_usd"] = position_size_usd
            result["position_size_coins"] = position_size_usd / entry_price

            # 8. 레버리지 계산
            # 자본 대비 포지션 크기 비율
            leverage_needed = position_size_usd / (self.account_balance * 0.95)  # 95% 자본 사용

            # 적절한 레버리지 단계로 반올림 (1, 2, 3, 5, 10, 20 등)
            if leverage_needed <= 1.0:
                leverage = 1.0
            elif leverage_needed <= 2.0:
                leverage = 2.0
            elif leverage_needed <= 3.0:
                leverage = 3.0
            elif leverage_needed <= 5.0:
                leverage = 5.0
            elif leverage_needed <= 10.0:
                leverage = 10.0
            else:
                leverage = min(self.config["max_leverage"], np.ceil(leverage_needed / 5) * 5)

            result["leverage"] = leverage

            # 9. 청산 가격 계산
            liquidation_buffer = self.config["liquidation_target_percentage"]

            if direction == "long":
                # 롱 포지션의 청산 가격 = 진입가 - (진입가 * (1 / 레버리지) * 청산버퍼)
                liquidation_price = entry_price - (entry_price * (1 / leverage) * liquidation_buffer)
            else:
                # 숏 포지션의 청산 가격 = 진입가 + (진입가 * (1 / 레버리지) * 청산버퍼)
                liquidation_price = entry_price + (entry_price * (1 / leverage) * liquidation_buffer)

            result["liquidation_price"] = liquidation_price

            # 10. 청산 가격이 손절가보다 나쁘면 레버리지 조정
            if (direction == "long" and liquidation_price >= stop_loss) or \
                    (direction == "short" and liquidation_price <= stop_loss):
                self.logger.warning("청산 가격이 손절가보다 나쁨, 레버리지 조정 필요")

                # 안전한 청산 가격을 위한 레버리지 재계산
                if direction == "long":
                    # 롱 포지션의 경우: 청산 가격이 손절가보다 낮아야 함
                    safe_leverage = (entry_price / (entry_price - stop_loss)) * liquidation_buffer * 0.9
                else:
                    # 숏 포지션의 경우: 청산 가격이 손절가보다 높아야 함
                    safe_leverage = (entry_price / (stop_loss - entry_price)) * liquidation_buffer * 0.9

                # 레버리지 상한 적용
                leverage = min(safe_leverage, self.config["max_leverage"])

                # 레버리지 단계로 반올림 (하향)
                if leverage <= 1.0:
                    leverage = 1.0
                elif leverage <= 2.0:
                    leverage = 2.0
                elif leverage <= 3.0:
                    leverage = 3.0
                elif leverage <= 5.0:
                    leverage = 5.0
                elif leverage <= 10.0:
                    leverage = 10.0
                else:
                    leverage = np.floor(leverage / 5) * 5

                result["leverage"] = leverage

                # 청산 가격 재계산
                if direction == "long":
                    liquidation_price = entry_price - (entry_price * (1 / leverage) * liquidation_buffer)
                else:
                    liquidation_price = entry_price + (entry_price * (1 / leverage) * liquidation_buffer)

                result["liquidation_price"] = liquidation_price

                # 포지션 크기 조정 (레버리지 변경으로 인한)
                position_size_usd = min(self.account_balance * leverage * 0.95, position_size_usd)
                result["position_size_usd"] = position_size_usd
                result["position_size_coins"] = position_size_usd / entry_price

            self.logger.info(
                f"포지션 크기 계산 완료: "
                f"크기=${result['position_size_usd']:.2f}, "
                f"레버리지={result['leverage']:.1f}x, "
                f"위험=${result['risk_amount_usd']:.2f} ({result['risk_percent']:.2f}%)"
            )

        except Exception as e:
            self.logger.error(f"포지션 크기 계산 중 오류 발생: {e}")

        return result

    def set_dynamic_tp_sl(
            self,
            entry_price: float,
            direction: str,
            atr_value: float,
            support_resistance: Optional[Dict[str, List[Dict[str, Any]]]] = None,
            liquidation_clusters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        동적 TP(이익실현)/SL(손절) 레벨을 설정합니다.

        Args:
            entry_price: 진입 가격
            direction: 포지션 방향 ('long' 또는 'short')
            atr_value: ATR 값
            support_resistance: 지지/저항 레벨
            liquidation_clusters: 청산 클러스터 데이터

        Returns:
            Dict: TP/SL 설정
        """
        self.logger.info(f"동적 TP/SL 설정: {direction}, 진입가: {entry_price}, ATR: {atr_value}")

        # 기본 결과 구조
        result = {
            "stop_loss": 0.0,
            "take_profit_levels": [],
            "tp_percentages": [],
            "sl_distance_percent": 0.0,
            "tp_distance_percent": [],
            "risk_reward_ratio": 0.0,
            "method_used": "atr"
        }

        try:
            # 1. 기본 ATR 기반 SL/TP 계산
            sl_atr_distance = atr_value * self.config["sl_atr_multiplier"]

            if direction == "long":
                stop_loss = entry_price - sl_atr_distance
                tp1 = entry_price + (atr_value * self.config["tp_atr_multiplier"]["first"])
                tp2 = entry_price + (atr_value * self.config["tp_atr_multiplier"]["second"])
                tp3 = entry_price + (atr_value * self.config["tp_atr_multiplier"]["third"])
            else:  # short
                stop_loss = entry_price + sl_atr_distance
                tp1 = entry_price - (atr_value * self.config["tp_atr_multiplier"]["first"])
                tp2 = entry_price - (atr_value * self.config["tp_atr_multiplier"]["second"])
                tp3 = entry_price - (atr_value * self.config["tp_atr_multiplier"]["third"])

            # 2. 지지/저항 레벨을 고려한 SL/TP 조정
            if support_resistance:
                if direction == "long":
                    # 롱 포지션: 손절을 가장 가까운 지지 레벨 아래로 조정
                    support_levels = sorted(support_resistance.get("support", []),
                                            key=lambda x: abs(x["price"] - entry_price))

                    if support_levels and abs(support_levels[0]["price"] - entry_price) / entry_price < 0.05:
                        sl_sr = support_levels[0]["price"] * (1 - self.config["sl_buffer_percent"] / 100)
                        # 더 보수적인(높은) 손절 선택
                        stop_loss = max(stop_loss, sl_sr)
                        result["method_used"] = "support_level"

                    # TP를 저항 레벨에 맞춤
                    resistance_levels = sorted(support_resistance.get("resistance", []),
                                               key=lambda x: x["price"])

                    if resistance_levels:
                        for i, r_level in enumerate(resistance_levels):
                            if r_level["price"] > entry_price:
                                if i == 0:
                                    tp1 = r_level["price"] * 0.99  # 저항 바로 아래
                                elif i == 1 or i == len(resistance_levels) - 1:
                                    tp2 = r_level["price"] * 0.99
                                elif i >= 2:
                                    tp3 = r_level["price"] * 0.99
                                    break

                else:  # short
                    # 숏 포지션: 손절을 가장 가까운 저항 레벨 위로 조정
                    resistance_levels = sorted(support_resistance.get("resistance", []),
                                               key=lambda x: abs(x["price"] - entry_price))

                    if resistance_levels and abs(resistance_levels[0]["price"] - entry_price) / entry_price < 0.05:
                        sl_sr = resistance_levels[0]["price"] * (1 + self.config["sl_buffer_percent"] / 100)
                        # 더 보수적인(낮은) 손절 선택
                        stop_loss = min(stop_loss, sl_sr)
                        result["method_used"] = "resistance_level"

                    # TP를 지지 레벨에 맞춤
                    support_levels = sorted(support_resistance.get("support", []),
                                            key=lambda x: x["price"], reverse=True)

                    if support_levels:
                        for i, s_level in enumerate(support_levels):
                            if s_level["price"] < entry_price:
                                if i == 0:
                                    tp1 = s_level["price"] * 1.01  # 지지 바로 위
                                elif i == 1 or i == len(support_levels) - 1:
                                    tp2 = s_level["price"] * 1.01
                                elif i >= 2:
                                    tp3 = s_level["price"] * 1.01
                                    break

            # 3. 청산 클러스터를 고려한 SL/TP 추가 조정
            if liquidation_clusters and "clusters" in liquidation_clusters:
                clusters = liquidation_clusters.get("clusters", [])

                if direction == "long":
                    # 롱 포지션의 경우 지지 클러스터(support) 중심
                    support_clusters = [c for c in clusters if c.get("type") == "support"]

                    # 진입가 아래 지지 클러스터 찾기 (손절 조정)
                    below_entry = [c for c in support_clusters
                                   if c.get("price_low", 0) < entry_price and c.get("intensity") in ["high", "medium"]]

                    if below_entry:
                        # 가장 높은 지지 클러스터 (진입가에 가장 가까운)
                        closest = max(below_entry, key=lambda x: x.get("price_high", 0))
                        sl_cluster = closest.get("price_low", 0) * 0.99  # 클러스터 아래로 1%

                        # 현재 손절가보다 높으면 업데이트
                        if sl_cluster > stop_loss:
                            stop_loss = sl_cluster
                            result["method_used"] = "liquidation_cluster"

                else:  # short
                    # 숏 포지션의 경우 저항 클러스터(resistance) 중심
                    resistance_clusters = [c for c in clusters if c.get("type") == "resistance"]

                    # 진입가 위 저항 클러스터 찾기 (손절 조정)
                    above_entry = [c for c in resistance_clusters
                                   if c.get("price_high", float('inf')) > entry_price and c.get("intensity") in ["high",
                                                                                                                 "medium"]]

                    if above_entry:
                        # 가장 낮은 저항 클러스터 (진입가에 가장 가까운)
                        closest = min(above_entry, key=lambda x: x.get("price_low", float('inf')))
                        sl_cluster = closest.get("price_high", 0) * 1.01  # 클러스터 위로 1%

                        # 현재 손절가보다 낮으면 업데이트
                        if sl_cluster < stop_loss:
                            stop_loss = sl_cluster
                            result["method_used"] = "liquidation_cluster"

            # 최종 TP/SL 설정
            take_profit_levels = [tp1, tp2, tp3]

            # 거리 계산
            sl_distance = abs(entry_price - stop_loss)
            sl_distance_percent = (sl_distance / entry_price) * 100

            tp_distances = [abs(tp - entry_price) for tp in take_profit_levels]
            tp_distance_percent = [(d / entry_price) * 100 for d in tp_distances]

            # 위험 대비 보상 비율 계산 (가중 평균 TP 사용)
            tp_weights = self.config["partial_tp_percentages"]
            weighted_tp_distance = sum(d * w for d, w in zip(tp_distances, tp_weights))
            risk_reward_ratio = weighted_tp_distance / sl_distance if sl_distance > 0 else 0

            # 최종 결과 저장
            result.update({
                "stop_loss": stop_loss,
                "take_profit_levels": take_profit_levels,
                "tp_percentages": self.config["partial_tp_percentages"],
                "sl_distance_percent": sl_distance_percent,
                "tp_distance_percent": tp_distance_percent,
                "risk_reward_ratio": risk_reward_ratio
            })

            self.logger.info(
                f"TP/SL 설정 완료: "
                f"SL={result['stop_loss']:.2f} ({result['sl_distance_percent']:.2f}%), "
                f"TP1={take_profit_levels[0]:.2f}, "
                f"TP2={take_profit_levels[1]:.2f}, "
                f"TP3={take_profit_levels[2]:.2f}, "
                f"R:R={result['risk_reward_ratio']:.2f}"
            )

        except Exception as e:
            self.logger.error(f"TP/SL 설정 중 오류 발생: {e}")

        return result

    def update_position_sl_tp(
            self,
            position_id: str,
            new_stop_loss: Optional[float] = None,
            new_take_profits: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """
        기존 포지션의 SL/TP를 업데이트합니다.

        Args:
            position_id: 포지션 ID
            new_stop_loss: 새 손절가
            new_take_profits: 새 이익실현 레벨 목록

        Returns:
            Dict: 업데이트 결과
        """
        self.logger.info(f"포지션 SL/TP 업데이트: ID={position_id}")

        # 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "updated_sl": False,
            "updated_tp": False,
            "old_sl": 0.0,
            "new_sl": 0.0,
            "old_tp": [],
            "new_tp": []
        }

        try:
            # 활성 포지션 찾기
            position = None
            for p in self.active_positions:
                if p.get("position_id") == position_id:
                    position = p
                    break

            if not position:
                self.logger.warning(f"ID가 {position_id}인 포지션을 찾을 수 없음")
                return result

            # 기존 값 저장
            result["old_sl"] = position.get("stop_loss", 0.0)
            result["old_tp"] = position.get("take_profit_levels", [])

            # SL 업데이트
            if new_stop_loss is not None:
                # 진입 방향 확인
                direction = position.get("direction", "long")
                entry_price = position.get("entry_price", 0.0)

                # 방향에 따른 유효성 검사
                if direction == "long" and new_stop_loss > entry_price:
                    self.logger.warning(f"롱 포지션에서 SL은 진입가보다 낮아야 함: {new_stop_loss} > {entry_price}")
                elif direction == "short" and new_stop_loss < entry_price:
                    self.logger.warning(f"숏 포지션에서 SL은 진입가보다 높아야 함: {new_stop_loss} < {entry_price}")
                else:
                    # SL 업데이트
                    position["stop_loss"] = new_stop_loss
                    result["updated_sl"] = True
                    result["new_sl"] = new_stop_loss

                    self.logger.info(f"SL 업데이트됨: {result['old_sl']} -> {new_stop_loss}")

            # TP 업데이트
            if new_take_profits is not None and len(new_take_profits) > 0:
                # 진입 방향 확인
                direction = position.get("direction", "long")
                entry_price = position.get("entry_price", 0.0)

                # 방향에 따른 유효성 검사
                valid_tps = True
                for tp in new_take_profits:
                    if direction == "long" and tp < entry_price:
                        self.logger.warning(f"롱 포지션에서 TP는 진입가보다 높아야 함: {tp} < {entry_price}")
                        valid_tps = False
                        break
                    elif direction == "short" and tp > entry_price:
                        self.logger.warning(f"숏 포지션에서 TP는 진입가보다 낮아야 함: {tp} > {entry_price}")
                        valid_tps = False
                        break

                if valid_tps:
                    # TP 업데이트
                    position["take_profit_levels"] = new_take_profits
                    result["updated_tp"] = True
                    result["new_tp"] = new_take_profits

                    self.logger.info(f"TP 업데이트됨: {result['old_tp']} -> {new_take_profits}")

            # 결과 업데이트
            result["success"] = result["updated_sl"] or result["updated_tp"]

        except Exception as e:
            self.logger.error(f"SL/TP 업데이트 중 오류 발생: {e}")

        return result

    def adjust_sl_to_breakeven(
            self,
            position_id: str,
            min_profit_percent: float = 1.0,
            buffer_percent: float = 0.2
    ) -> Dict[str, Any]:
        """
        수익이 일정 수준 이상일 때 손절가를 원금 수준으로 조정합니다.

        Args:
            position_id: 포지션 ID
            min_profit_percent: 최소 수익 비율 (원금 이동 기준)
            buffer_percent: 손절가 버퍼 (%)

        Returns:
            Dict: 조정 결과
        """
        self.logger.info(f"손절가 원금 이동 시도: 포지션={position_id}, 최소수익={min_profit_percent}%")

        # 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "old_sl": 0.0,
            "new_sl": 0.0,
            "entry_price": 0.0,
            "current_price": 0.0,
            "current_profit_percent": 0.0
        }

        try:
            # 활성 포지션 찾기
            position = None
            for p in self.active_positions:
                if p.get("position_id") == position_id:
                    position = p
                    break

            if not position:
                self.logger.warning(f"ID가 {position_id}인 포지션을 찾을 수 없음")
                return result

            # 현재 가격 조회
            current_price = self.get_current_price()
            if current_price <= 0:
                self.logger.error("현재 가격을 가져올 수 없음")
                return result

            # 포지션 정보
            entry_price = position.get("entry_price", 0.0)
            direction = position.get("direction", "long")
            old_sl = position.get("stop_loss", 0.0)

            # 현재 수익률 계산
            if direction == "long":
                profit_percent = (current_price - entry_price) / entry_price * 100
            else:  # short
                profit_percent = (entry_price - current_price) / entry_price * 100

            # 원금 수준 SL 이동 조건 확인
            if profit_percent >= min_profit_percent:
                # 새 손절가 계산 (원금 + 약간의 버퍼)
                if direction == "long":
                    new_sl = entry_price * (1 + buffer_percent / 100)

                    # 기존 손절가보다 높을 때만 업데이트
                    if new_sl > old_sl:
                        position["stop_loss"] = new_sl
                        result["success"] = True
                else:  # short
                    new_sl = entry_price * (1 - buffer_percent / 100)

                    # 기존 손절가보다 낮을 때만 업데이트
                    if new_sl < old_sl:
                        position["stop_loss"] = new_sl
                        result["success"] = True

                if result["success"]:
                    result.update({
                        "old_sl": old_sl,
                        "new_sl": position["stop_loss"],
                        "entry_price": entry_price,
                        "current_price": current_price,
                        "current_profit_percent": profit_percent
                    })

                    self.logger.info(
                        f"손절가 원금으로 이동: "
                        f"수익={profit_percent:.2f}%, "
                        f"SL 변경: {old_sl} -> {position['stop_loss']}"
                    )
            else:
                self.logger.info(f"수익 불충분: {profit_percent:.2f}% < {min_profit_percent}%, 변경 없음")
                result["current_profit_percent"] = profit_percent

        except Exception as e:
            self.logger.error(f"손절가 원금 이동 중 오류 발생: {e}")

        return result

    def trailing_stop_loss(
            self,
            position_id: str,
            trail_percent: float = 1.5
    ) -> Dict[str, Any]:
        """
        트레일링 손절가를 설정합니다.

        Args:
            position_id: 포지션 ID
            trail_percent: 트레일링 거리 (%)

        Returns:
            Dict: 트레일링 결과
        """
        self.logger.info(f"트레일링 손절 시도: 포지션={position_id}, 트레일={trail_percent}%")

        # 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "old_sl": 0.0,
            "new_sl": 0.0,
            "highest_price": 0.0,
            "lowest_price": 0.0,
            "current_price": 0.0
        }

        try:
            # 활성 포지션 찾기
            position = None
            for p in self.active_positions:
                if p.get("position_id") == position_id:
                    position = p
                    break

            if not position:
                self.logger.warning(f"ID가 {position_id}인 포지션을 찾을 수 없음")
                return result

            # 현재 가격 조회
            current_price = self.get_current_price()
            if current_price <= 0:
                self.logger.error("현재 가격을 가져올 수 없음")
                return result

            # 포지션 정보
            entry_price = position.get("entry_price", 0.0)
            direction = position.get("direction", "long")
            old_sl = position.get("stop_loss", 0.0)

            # 최고/최저 가격 업데이트
            if direction == "long":
                # 롱 포지션은 최고가 추적
                highest_price = position.get("highest_price", entry_price)

                if current_price > highest_price:
                    highest_price = current_price
                    position["highest_price"] = highest_price

                    # 트레일링 SL 계산
                    new_sl = highest_price * (1 - trail_percent / 100)

                    # 기존 SL보다 높을 때만 업데이트
                    if new_sl > old_sl:
                        position["stop_loss"] = new_sl
                        result.update({
                            "success": True,
                            "old_sl": old_sl,
                            "new_sl": new_sl,
                            "highest_price": highest_price,
                            "current_price": current_price
                        })

                        self.logger.info(
                            f"롱 트레일링 SL 업데이트: "
                            f"최고가={highest_price}, "
                            f"SL 변경: {old_sl} -> {new_sl}"
                        )

                result["highest_price"] = highest_price

            else:  # short
                # 숏 포지션은 최저가 추적
                lowest_price = position.get("lowest_price", entry_price)

                if current_price < lowest_price:
                    lowest_price = current_price
                    position["lowest_price"] = lowest_price

                    # 트레일링 SL 계산
                    new_sl = lowest_price * (1 + trail_percent / 100)

                    # 기존 SL보다 낮을 때만 업데이트
                    if new_sl < old_sl:
                        position["stop_loss"] = new_sl
                        result.update({
                            "success": True,
                            "old_sl": old_sl,
                            "new_sl": new_sl,
                            "lowest_price": lowest_price,
                            "current_price": current_price
                        })

                        self.logger.info(
                            f"숏 트레일링 SL 업데이트: "
                            f"최저가={lowest_price}, "
                            f"SL 변경: {old_sl} -> {new_sl}"
                        )

                result["lowest_price"] = lowest_price

            result["current_price"] = current_price

        except Exception as e:
            self.logger.error(f"트레일링 손절 중 오류 발생: {e}")

        return result

    def validate_risk_parameters(
            self,
            entry_price: float,
            stop_loss: float,
            direction: str,
            take_profit_levels: List[float]
    ) -> Dict[str, Any]:
        """
        위험 관리 파라미터의 유효성을 검증합니다.

        Args:
            entry_price: 진입 가격
            stop_loss: 손절 가격
            direction: 포지션 방향 ('long' 또는 'short')
            take_profit_levels: 이익실현 레벨 목록

        Returns:
            Dict: 검증 결과
        """
        self.logger.info(f"위험 파라미터 검증: {direction}, 진입가: {entry_price}, SL: {stop_loss}")

        # 결과 구조
        result = {
            "valid": True,
            "issues": [],
            "risk_reward_ratio": 0.0,
            "sl_distance_percent": 0.0,
            "tp_distance_percent": []
        }

        try:
            issues = []

            # 1. 방향에 따른 SL 위치 검증
            if direction == "long" and stop_loss >= entry_price:
                issues.append("롱 포지션에서 손절가는 진입가보다 낮아야 합니다")
                result["valid"] = False
            elif direction == "short" and stop_loss <= entry_price:
                issues.append("숏 포지션에서 손절가는 진입가보다 높아야 합니다")
                result["valid"] = False

            # 2. SL 거리 계산 및 검증
            sl_distance = abs(entry_price - stop_loss)
            sl_distance_percent = (sl_distance / entry_price) * 100

            # SL이 너무 가까운지 확인 (0.5% 미만)
            if sl_distance_percent < 0.5:
                issues.append(f"손절가가 너무 가깝습니다: {sl_distance_percent:.2f}% < 0.5%")
                result["valid"] = False

            # SL이 너무 먼지 확인 (15% 초과)
            if sl_distance_percent > 15:
                issues.append(f"손절가가 너무 멉니다: {sl_distance_percent:.2f}% > 15%")

            # 3. TP 레벨 검증
            if take_profit_levels:
                tp_distances = []
                for i, tp in enumerate(take_profit_levels):
                    # 방향에 따른 TP 위치 검증
                    if direction == "long" and tp <= entry_price:
                        issues.append(f"롱 포지션에서 TP{i + 1}({tp})은 진입가보다 높아야 합니다")
                        result["valid"] = False
                    elif direction == "short" and tp >= entry_price:
                        issues.append(f"숏 포지션에서 TP{i + 1}({tp})은 진입가보다 낮아야 합니다")
                        result["valid"] = False

                    # TP 거리 계산
                    tp_distance = abs(tp - entry_price)
                    tp_distance_percent = (tp_distance / entry_price) * 100
                    tp_distances.append(tp_distance_percent)

                # 위험 대비 보상 비율 계산 (TP1 기준)
                if tp_distances:
                    first_tp_distance = tp_distances[0]
                    risk_reward_ratio = first_tp_distance / sl_distance_percent if sl_distance_percent > 0 else 0

                    # 최소 R:R 검증
                    min_rr = self.config["min_risk_reward_ratio"]
                    if risk_reward_ratio < min_rr:
                        issues.append(f"위험 대비 보상 비율이 낮습니다: {risk_reward_ratio:.2f} < {min_rr}")
                        # 이는 경고만 하고 거래를 막지는 않음

                    result["risk_reward_ratio"] = risk_reward_ratio
                    result["tp_distance_percent"] = tp_distances

            # 결과 업데이트
            result["issues"] = issues
            result["sl_distance_percent"] = sl_distance_percent

            if issues:
                issues_str = "\n - " + "\n - ".join(issues)
                self.logger.warning(f"위험 파라미터 검증 문제 발견: {issues_str}")
            else:
                self.logger.info("위험 파라미터 검증 완료: 모든 파라미터 유효")

        except Exception as e:
            result["valid"] = False
            result["issues"].append(f"검증 중 오류 발생: {str(e)}")
            self.logger.error(f"위험 파라미터 검증 중 오류 발생: {e}")

        return result

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

    def update_trade_history(
            self,
            trade_result: Dict[str, Any]
    ) -> None:
        """
        거래 결과를 기록하고 통계를 업데이트합니다.

        Args:
            trade_result: 거래 결과 데이터
        """
        try:
            # 거래 이력에 추가
            self.trade_history.append(trade_result)

            # 수익/손실 여부
            is_profit = trade_result.get("pnl_amount", 0) > 0

            # 승/패 스트릭 업데이트
            if is_profit:
                self.current_winning_streak += 1
                self.current_losing_streak = 0
            else:
                self.current_losing_streak += 1
                self.current_winning_streak = 0

            # 손익 업데이트
            pnl_amount = trade_result.get("pnl_amount", 0)
            self.daily_pnl += pnl_amount
            self.weekly_pnl += pnl_amount
            self.monthly_pnl += pnl_amount

            # 계정 잔고 업데이트
            self.account_balance += pnl_amount

            # 최대 잔고 및 낙폭 업데이트
            if self.account_balance > self.max_account_balance:
                self.max_account_balance = self.account_balance
                self.current_drawdown = 0
            else:
                self.current_drawdown = 1 - (self.account_balance / self.max_account_balance)

            # 활성 포지션 목록에서 제거
            position_id = trade_result.get("position_id")
            if position_id:
                self.active_positions = [p for p in self.active_positions if p.get("position_id") != position_id]

            self.logger.info(
                f"거래 결과 기록: "
                f"손익=${pnl_amount:.2f}, "
                f"현재 잔고=${self.account_balance:.2f}, "
                f"낙폭={self.current_drawdown:.2f}, "
                f"연승={self.current_winning_streak}, "
                f"연패={self.current_losing_streak}"
            )

        except Exception as e:
            self.logger.error(f"거래 이력 업데이트 중 오류 발생: {e}")

    def check_daily_limits(self) -> Dict[str, Any]:
        """
        일일 거래 한도를 확인합니다.

        Returns:
            Dict: 한도 확인 결과
        """
        result = {
            "can_trade": True,
            "limits_reached": [],
            "daily_pnl": self.daily_pnl,
            "daily_pnl_percent": 0.0,
            "weekly_pnl": self.weekly_pnl,
            "weekly_pnl_percent": 0.0,
            "monthly_pnl": self.monthly_pnl,
            "monthly_pnl_percent": 0.0,
            "drawdown": self.current_drawdown
        }

        try:
            # 백분율 계산
            daily_pnl_percent = (self.daily_pnl / self.account_balance) * 100
            weekly_pnl_percent = (self.weekly_pnl / self.account_balance) * 100
            monthly_pnl_percent = (self.monthly_pnl / self.account_balance) * 100

            result.update({
                "daily_pnl_percent": daily_pnl_percent,
                "weekly_pnl_percent": weekly_pnl_percent,
                "monthly_pnl_percent": monthly_pnl_percent
            })

            # 한도 확인
            limits = []

            # 일일 손실 한도
            if self.daily_pnl < -self.config["daily_loss_limit"] * self.account_balance:
                limits.append(
                    f"일일 손실 한도 초과: {daily_pnl_percent:.2f}% (한도: {-self.config['daily_loss_limit'] * 100:.2f}%)")

            # 주간 손실 한도
            if self.weekly_pnl < -self.config["weekly_loss_limit"] * self.account_balance:
                limits.append(
                    f"주간 손실 한도 초과: {weekly_pnl_percent:.2f}% (한도: {-self.config['weekly_loss_limit'] * 100:.2f}%)")

            # 월간 손실 한도
            if self.monthly_pnl < -self.config["monthly_loss_limit"] * self.account_balance:
                limits.append(
                    f"월간 손실 한도 초과: {monthly_pnl_percent:.2f}% (한도: {-self.config['monthly_loss_limit'] * 100:.2f}%)")

            # 자본 감소 임계값
            if self.current_drawdown > self.config["drawdown_reduction_threshold"]:
                limits.append(
                    f"낙폭 임계값 초과: {self.current_drawdown:.2f} (임계값: {self.config['drawdown_reduction_threshold']:.2f})")

            # 거래 가능 여부 결정
            if limits:
                result["can_trade"] = False
                result["limits_reached"] = limits
                self.logger.warning(f"거래 한도 초과: {', '.join(limits)}")
            else:
                self.logger.info("거래 한도 확인 완료: 모든 한도 내 정상")

        except Exception as e:
            self.logger.error(f"거래 한도 확인 중 오류 발생: {e}")
            result["can_trade"] = False
            result["limits_reached"].append(f"확인 중 오류: {str(e)}")

        return result

    def reset_daily_stats(self) -> None:
        """일일 통계를 초기화합니다."""
        self.logger.info(f"일일 통계 초기화: 이전 일일 손익=${self.daily_pnl:.2f}")
        self.daily_pnl = 0.0

    def reset_weekly_stats(self) -> None:
        """주간 통계를 초기화합니다."""
        self.logger.info(f"주간 통계 초기화: 이전 주간 손익=${self.weekly_pnl:.2f}")
        self.weekly_pnl = 0.0

    def reset_monthly_stats(self) -> None:
        """월간 통계를 초기화합니다."""
        self.logger.info(f"월간 통계 초기화: 이전 월간 손익=${self.monthly_pnl:.2f}")
        self.monthly_pnl = 0.0

    def process_entry_decision(
            self,
            entry_signal: Dict[str, Any],
            market_condition: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        진입 결정을 처리하고 최적의 위험 관리 파라미터를 계산합니다.

        Args:
            entry_signal: 진입 신호 데이터
            market_condition: 시장 상황 데이터

        Returns:
            Dict: 위험 관리 파라미터를 포함한 진입 정보
        """
        self.logger.info(f"진입 결정 처리 중: {entry_signal.get('direction')}, 전략: {entry_signal.get('strategy_id')}")

        # 기본 결과 구조
        result = {
            "can_proceed": False,
            "reason": "",
            "entry_signal": entry_signal,
            "position_size": {},
            "tp_sl": {},
            "validation": {},
            "limits_check": {}
        }

        try:
            # 1. 거래 한도 확인
            limits_check = self.check_daily_limits()
            result["limits_check"] = limits_check

            if not limits_check["can_trade"]:
                result["reason"] = "거래 한도 초과"
                return result

            # 2. 진입 신호 검증
            if not entry_signal or entry_signal.get("direction") == "none":
                result["reason"] = "유효한 진입 신호 없음"
                return result

            # 3. 필수 요소 확인
            required_fields = ["direction", "entry_price", "stop_loss_level"]
            missing_fields = [f for f in required_fields if f not in entry_signal or not entry_signal[f]]

            if missing_fields:
                result["reason"] = f"필수 요소 누락: {', '.join(missing_fields)}"
                return result

            # 4. 진입 정보 추출
            direction = entry_signal.get("direction")
            entry_price = entry_signal.get("entry_price", 0.0)
            stop_loss = entry_signal.get("stop_loss_level", 0.0)
            strategy = entry_signal.get("strategy_id", "unknown")
            signal_strength = entry_signal.get("strength", 70.0)
            timeframe = entry_signal.get("timeframe", "1h")
            atr_value = entry_signal.get("atr_value", 0.0)

            # 5. 지지/저항 레벨 가져오기
            support_resistance = self.get_support_resistance_levels()

            # 6. 청산 클러스터 가져오기
            liquidation_clusters = self.get_liquidation_clusters()

            # 7. TP/SL 설정 계산
            tp_sl = self.set_dynamic_tp_sl(
                entry_price,
                direction,
                atr_value,
                support_resistance,
                liquidation_clusters
            )
            result["tp_sl"] = tp_sl

            # 8. 위험 파라미터 검증
            validation = self.validate_risk_parameters(
                entry_price,
                tp_sl["stop_loss"],
                direction,
                tp_sl["take_profit_levels"]
            )
            result["validation"] = validation

            if not validation["valid"]:
                result["reason"] = f"위험 파라미터 유효성 검증 실패: {validation['issues']}"
                return result

            # 9. 포지션 크기 계산
            position_size = self.calculate_position_size(
                entry_price,
                tp_sl["stop_loss"],
                direction,
                strategy,
                signal_strength,
                market_condition
            )
            result["position_size"] = position_size

            # 모든 검증 통과
            result["can_proceed"] = True

        except Exception as e:
            self.logger.error(f"진입 결정 처리 중 오류 발생: {e}")
            result["reason"] = f"처리 중 오류 발생: {str(e)}"

        return result

    def get_support_resistance_levels(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        지지/저항 레벨을 가져옵니다.

        Returns:
            Dict: 지지/저항 레벨 데이터
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
            self.logger.error(f"지지/저항 레벨 조회 중 오류 발생: {e}")
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
                try:
                    return json.loads(cached_data)
                except json.JSONDecodeError:
                    self.logger.error("청산 클러스터 JSON 파싱 오류")

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

            self.logger.warning("청산 클러스터 데이터가 없습니다")
            return {}

        except Exception as e:
            self.logger.error(f"청산 클러스터 조회 중 오류 발생: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

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

    def finalize_tp_sl(
            self,
            signal: Dict[str, Any],
            market_condition: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        전략에서 제안된 TP/SL 값을 평가하고 최종 값을 결정합니다.

        Args:
            signal: 전략에서 생성된 신호
            market_condition: 현재 시장 상황

        Returns:
            Dict: 최종 TP/SL 값이 포함된 결과
        """
        self.logger.info(f"TP/SL 값 최종화 중: {signal.get('direction')}, 전략: {signal.get('strategy_id', '미지정')}")

        # 기본 결과 구조
        result = {
            "stop_loss": 0.0,
            "take_profit_levels": [],
            "tp_percentages": [],
            "adjusted_reason": ""
        }

        try:
            # 전략 제안 값 추출
            entry_price = signal.get("entry_price", 0.0)
            suggested_sl = signal.get("suggested_stop_loss", 0.0)
            suggested_tp_levels = signal.get("suggested_take_profit_levels", [])
            direction = signal.get("direction", "none")
            atr_value = signal.get("atr_value", 0.0)

            if entry_price <= 0 or suggested_sl <= 0 or not suggested_tp_levels:
                self.logger.warning("유효한 진입/TP/SL 값이 없습니다")
                return result

            # 1. 손절 검증 및 조정
            final_sl = suggested_sl
            sl_adjustment_reason = []

            # 최소 스탑 거리 확인
            min_sl_distance = atr_value * 0.5  # 최소 ATR의 절반
            actual_sl_distance = abs(entry_price - final_sl)

            if actual_sl_distance < min_sl_distance:
                # 스탑이 너무 가까우면 조정
                if direction == "long":
                    final_sl = entry_price - min_sl_distance
                else:
                    final_sl = entry_price + min_sl_distance
                sl_adjustment_reason.append(f"최소 스탑 거리 조정 (ATR의 0.5배)")

            # 최대 스탑 거리 확인
            max_sl_distance = atr_value * 3.0  # 최대 ATR의 3배

            if actual_sl_distance > max_sl_distance:
                # 스탑이 너무 멀면 조정
                if direction == "long":
                    final_sl = entry_price - max_sl_distance
                else:
                    final_sl = entry_price + max_sl_distance
                sl_adjustment_reason.append(f"최대 스탑 거리 조정 (ATR의 3배)")

            # 2. 이익실현 검증 및 조정
            final_tp_levels = []
            final_tp_percentages = []

            for i, (tp_price, tp_percentage) in enumerate(suggested_tp_levels):
                # 최소 R:R 검증
                risk = abs(entry_price - final_sl)
                reward = abs(tp_price - entry_price)
                rr_ratio = reward / risk if risk > 0 else 0

                # 첫 번째 TP의 R:R이 너무 낮으면 조정
                if i == 0 and rr_ratio < self.config["min_risk_reward_ratio"]:
                    if direction == "long":
                        tp_price = entry_price + (risk * self.config["min_risk_reward_ratio"])
                    else:
                        tp_price = entry_price - (risk * self.config["min_risk_reward_ratio"])
                    sl_adjustment_reason.append(f"첫 번째 TP의 R:R 비율 조정 (최소 {self.config['min_risk_reward_ratio']})")

                final_tp_levels.append(tp_price)
                final_tp_percentages.append(tp_percentage)

            # 시장 상황에 따른 추가 조정
            if market_condition:
                volatility_level = market_condition.get("volatility", {}).get("level", "medium")

                # 고변동성 시장에서는 TP를 더 멀리, SL을 더 가깝게
                if volatility_level in ["high", "very_high"]:
                    for i in range(len(final_tp_levels)):
                        if direction == "long":
                            final_tp_levels[i] *= 1.1  # 10% 증가
                        else:
                            final_tp_levels[i] *= 0.9  # 10% 감소

                    sl_adjustment_reason.append(f"고변동성 시장 조정")

            # 최종 결과 저장
            result.update({
                "stop_loss": final_sl,
                "take_profit_levels": final_tp_levels,
                "tp_percentages": final_tp_percentages,
                "adjusted_reason": ", ".join(sl_adjustment_reason) if sl_adjustment_reason else "제안값 수용"
            })

            self.logger.info(
                f"TP/SL 최종화 완료: "
                f"SL={final_sl:.2f}, "
                f"TP={[f'{tp:.2f}' for tp in final_tp_levels]}, "
                f"조정 이유: {result['adjusted_reason']}"
            )

        except Exception as e:
            self.logger.error(f"TP/SL 최종화 중 오류 발생: {e}")

        return result


# 직접 실행 시 관리자 시작
if __name__ == "__main__":
    risk_manager = RiskManager("BTC/USDT", 10000.0)

    try:
        # 샘플 진입 신호
        entry_signal = {
            "direction": "long",
            "entry_price": 30000.0,
            "stop_loss_level": 29000.0,
            "strategy_id": "trend_following",
            "strength": 75.0,
            "timeframe": "4h",
            "atr_value": 500.0
        }

        # 위험 관리 처리
        result = risk_manager.process_entry_decision(entry_signal)

        if result["can_proceed"]:
            print("진입 결정 처리 완료:")
            print(f"포지션 크기: ${result['position_size']['position_size_usd']:.2f}")
            print(f"레버리지: {result['position_size']['leverage']:.1f}x")
            print(f"위험 금액: ${result['position_size']['risk_amount_usd']:.2f}")
            print(f"손절가: {result['tp_sl']['stop_loss']:.2f}")
            print(f"TP 레벨: {[f'{tp:.2f}' for tp in result['tp_sl']['take_profit_levels']]}")
            print(f"R:R 비율: {result['tp_sl']['risk_reward_ratio']:.2f}")
        else:
            print(f"진입 불가: {result['reason']}")

    except Exception as e:
        print(f"실행 오류: {e}")
    finally:
        risk_manager.close()
