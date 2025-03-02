# strategies/breakout_strategy.py
"""
돌파 전략

이 모듈은 돌파 전략을 구현합니다.
중요 가격 레벨(지지/저항)을 돌파할 때 해당 방향으로 진입하는 전략입니다.
"""

import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from strategies.base_strategy import BaseStrategy


class BreakoutStrategy(BaseStrategy):
    """
    돌파 전략

    주요 개념:
    - 중요 지지/저항 레벨 식별
    - 돌파 확인 및 필터링
    - 가짜 돌파 방지를 위한 확인 요소
    - 볼륨 확인을 통한 돌파 강도 검증
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            config: Optional[Dict[str, Any]] = None
    ):
        """
        돌파 전략 초기화

        Args:
            symbol: 거래 심볼
            config: 전략 설정
        """
        super().__init__("breakout", "돌파 전략", symbol)

        # 기본 설정
        self.default_config = {
            "timeframes": ["1d", "4h", "1h"],  # 분석 시간프레임
            "lookback_periods": 30,  # 분석 기간 (캔들 수)
            "breakout_pct": 0.5,  # 돌파 기준 (%)
            "volume_multiplier": 1.5,  # 돌파 시 볼륨 배수
            "consolidation_periods": 5,  # 최소 통합 기간
            "min_touches": 2,  # 레벨 유효성을 위한 최소 터치 횟수
            "confirmation_periods": 2,  # 돌파 확인을 위한 캔들 수
            "atr_multiplier": 2.0,  # ATR 배수 (변동성 기준)
            "false_breakout_buffer": 0.3,  # 가짜 돌파 버퍼 (%)
            "min_consolidation_range": 3.0,  # 최소 통합 범위 (%)
            "max_consolidation_range": 15.0,  # 최대 통합 범위 (%)
            "tp_multiplier": [1.0, 2.0, 3.0],  # 이익 실현 배수
            "sl_multiplier": 1.0,  # 손절 배수
            "risk_per_trade": 0.01,  # 거래당 위험률 (계정의 %)
            "max_leverage": 10  # 최대 레버리지
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # 지지/저항 레벨 캐시
        self.sr_levels_cache = {}

        # 돌파 이벤트 캐시
        self.breakout_events = {}

        self.logger.info(f"돌파 전략 설정됨: {self.config}")

    def identify_consolidation_zones(
            self,
            df: pd.DataFrame,
            min_periods: int = 5
    ) -> List[Dict[str, Any]]:
        """
        가격 통합 구간을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            min_periods: 최소 통합 기간

        Returns:
            List[Dict]: 통합 구간 목록
        """
        if df.empty or len(df) < min_periods:
            return []

        # 기준 변동성 (ATR 기반)
        atr = df['atr'].iloc[-1]
        avg_price = df['close'].mean()

        # 기준 변동성 비율
        volatility_pct = atr / avg_price * 100

        # 변동성에 따른 통합 범위 조정
        min_range_pct = max(self.config["min_consolidation_range"], volatility_pct * 0.5)
        max_range_pct = min(self.config["max_consolidation_range"], volatility_pct * 5)

        consolidation_zones = []

        # 슬라이딩 윈도우로 통합 구간 탐색
        for start_idx in range(len(df) - min_periods + 1):
            end_idx = start_idx + min_periods - 1

            # 윈도우 내 가격 범위
            window = df.iloc[start_idx:end_idx + 1]
            price_high = window['high'].max()
            price_low = window['low'].min()

            # 범위 계산 (%)
            price_range_pct = (price_high - price_low) / price_low * 100

            # 통합 구간 조건: 적절한 범위 내의 가격 움직임
            if min_range_pct <= price_range_pct <= max_range_pct:
                # 구간 경계 터치 확인
                upper_touches = sum(1 for i in range(start_idx, end_idx + 1)
                                    if abs(df.iloc[i]['high'] - price_high) / price_high < 0.001)

                lower_touches = sum(1 for i in range(start_idx, end_idx + 1)
                                    if abs(df.iloc[i]['low'] - price_low) / price_low < 0.001)

                # 충분한 터치가 있는지 확인
                if upper_touches >= self.config["min_touches"] and lower_touches >= self.config["min_touches"]:
                    consolidation_zones.append({
                        "start_idx": start_idx,
                        "end_idx": end_idx,
                        "price_high": price_high,
                        "price_low": price_low,
                        "upper_touches": upper_touches,
                        "lower_touches": lower_touches,
                        "range_pct": price_range_pct
                    })

        # 중복 구간 병합
        if consolidation_zones:
            merged_zones = []
            current_zone = consolidation_zones[0]

            for zone in consolidation_zones[1:]:
                # 구간이 겹치는지 확인
                if zone["start_idx"] <= current_zone["end_idx"]:
                    # 병합
                    current_zone["end_idx"] = max(current_zone["end_idx"], zone["end_idx"])
                    current_zone["price_high"] = max(current_zone["price_high"], zone["price_high"])
                    current_zone["price_low"] = min(current_zone["price_low"], zone["price_low"])
                    current_zone["upper_touches"] += zone["upper_touches"]
                    current_zone["lower_touches"] += zone["lower_touches"]
                    current_zone["range_pct"] = (current_zone["price_high"] - current_zone["price_low"]) / current_zone[
                        "price_low"] * 100
                else:
                    merged_zones.append(current_zone)
                    current_zone = zone

            merged_zones.append(current_zone)
            return merged_zones

        return consolidation_zones

    def detect_breakout(
            self,
            df: pd.DataFrame,
            consolidation_zone: Dict[str, Any],
            confirmation_periods: int = 2
    ) -> Dict[str, Any]:
        """
        특정 통합 구간에서의 돌파를 감지합니다.

        Args:
            df: OHLCV 데이터프레임
            consolidation_zone: 통합 구간 정보
            confirmation_periods: 돌파 확인 캔들 수

        Returns:
            Dict: 돌파 감지 결과
        """
        # 기본 결과
        result = {
            "breakout": False,
            "direction": "none",
            "price": 0,
            "volume_surge": False,
            "false_breakout": False,
            "strength": 0
        }

        # 통합 구간 정보
        zone_end_idx = consolidation_zone["end_idx"]
        zone_high = consolidation_zone["price_high"]
        zone_low = consolidation_zone["price_low"]

        # 통합 구간 이후 데이터가 충분한지 확인
        if zone_end_idx + confirmation_periods >= len(df):
            return result

        # 통합 구간의 평균 볼륨
        zone_avg_volume = df.iloc[consolidation_zone["start_idx"]:consolidation_zone["end_idx"] + 1]['volume'].mean()

        # 돌파 캔들
        breakout_candle = df.iloc[zone_end_idx + 1]

        # 상향 돌파 확인
        upside_breakout = breakout_candle['close'] > zone_high * (1 + self.config["breakout_pct"] / 100)

        # 하향 돌파 확인
        downside_breakout = breakout_candle['close'] < zone_low * (1 - self.config["breakout_pct"] / 100)

        # 돌파 없음
        if not upside_breakout and not downside_breakout:
            return result

        # 돌파 방향
        direction = "up" if upside_breakout else "down"

        # 볼륨 급증 확인
        volume_surge = breakout_candle['volume'] > zone_avg_volume * self.config["volume_multiplier"]

        # 돌파 확인 기간
        confirmation_data = df.iloc[zone_end_idx + 1:zone_end_idx + 1 + confirmation_periods]

        # 상향 돌파 확인
        if upside_breakout:
            # 확인 기간 동안 고점 위에 유지
            confirmed = all(candle['close'] > zone_high for _, candle in confirmation_data.iterrows())

            # 가짜 돌파 확인 (돌파 후 빠르게 하락)
            false_breakout = any(candle['close'] < zone_high * (1 - self.config["false_breakout_buffer"] / 100)
                                 for _, candle in confirmation_data.iterrows())

            if confirmed and not false_breakout:
                # 돌파 강도 계산
                breakout_pct = (breakout_candle['close'] - zone_high) / zone_high * 100
                volume_factor = breakout_candle['volume'] / zone_avg_volume

                strength = min(100, breakout_pct * 10 + (volume_factor - 1) * 20)

                result.update({
                    "breakout": True,
                    "direction": "up",
                    "price": breakout_candle['close'],
                    "volume_surge": volume_surge,
                    "false_breakout": false_breakout,
                    "strength": strength
                })

        # 하향 돌파 확인
        elif downside_breakout:
            # 확인 기간 동안 저점 아래에 유지
            confirmed = all(candle['close'] < zone_low for _, candle in confirmation_data.iterrows())

            # 가짜 돌파 확인 (돌파 후 빠르게 상승)
            false_breakout = any(candle['close'] > zone_low * (1 + self.config["false_breakout_buffer"] / 100)
                                 for _, candle in confirmation_data.iterrows())

            if confirmed and not false_breakout:
                # 돌파 강도 계산
                breakout_pct = (zone_low - breakout_candle['close']) / zone_low * 100
                volume_factor = breakout_candle['volume'] / zone_avg_volume

                strength = min(100, breakout_pct * 10 + (volume_factor - 1) * 20)

                result.update({
                    "breakout": True,
                    "direction": "down",
                    "price": breakout_candle['close'],
                    "volume_surge": volume_surge,
                    "false_breakout": false_breakout,
                    "strength": strength
                })

        return result

    def find_key_levels(self, df: pd.DataFrame) -> Tuple[List[float], List[float]]:
        """
        중요 지지/저항 레벨을 찾습니다.

        Args:
            df: OHLCV 데이터프레임

        Returns:
            Tuple[List[float], List[float]]: (지지 레벨 목록, 저항 레벨 목록)
        """
        # 지지/저항 레벨 가져오기
        support_resistance = self.get_support_resistance_levels()

        support_levels = [level.get('price', 0) for level in support_resistance.get('support', [])]
        resistance_levels = [level.get('price', 0) for level in support_resistance.get('resistance', [])]

        # 레벨이 없으면 직접 계산
        if not support_levels and not resistance_levels:
            # 피크 및 트로프 찾기
            highs = df['high'].values
            lows = df['low'].values

            # 고점 식별 (5개 캔들 윈도우에서 중앙값이 최대값)
            peaks = []
            for i in range(2, len(df) - 2):
                if highs[i] == max(highs[i - 2:i + 3]):
                    peaks.append(highs[i])

            # 저점 식별 (5개 캔들 윈도우에서 중앙값이 최소값)
            troughs = []
            for i in range(2, len(df) - 2):
                if lows[i] == min(lows[i - 2:i + 3]):
                    troughs.append(lows[i])

            # 레벨 클러스터링 (비슷한 가격끼리 그룹화)
            def cluster_levels(levels, threshold_pct=0.01):
                if not levels:
                    return []

                clustered = []
                current_cluster = [levels[0]]

                for i in range(1, len(levels)):
                    if abs(levels[i] - current_cluster[0]) / current_cluster[0] <= threshold_pct:
                        current_cluster.append(levels[i])
                    else:
                        # 이전 클러스터 완료, 새 클러스터 시작
                        clustered.append(sum(current_cluster) / len(current_cluster))
                        current_cluster = [levels[i]]

                # 마지막 클러스터 추가
                if current_cluster:
                    clustered.append(sum(current_cluster) / len(current_cluster))

                return clustered

            # 클러스터링 적용
            resistance_levels = cluster_levels(peaks)
            support_levels = cluster_levels(troughs)

        return support_levels, resistance_levels

    def scan_for_breakouts(
            self,
            timeframe: str,
            limit: int = 100
    ) -> Dict[str, Any]:
        """
        특정 시간프레임에서 돌파 가능성을 스캔합니다.

        Args:
            timeframe: 시간프레임
            limit: 분석할 캔들 수

        Returns:
            Dict: 돌파 스캔 결과
        """
        self.logger.info(f"{timeframe} 시간프레임 돌파 스캔 중...")

        # 스캔 결과 초기화
        scan_result = {
            "timeframe": timeframe,
            "breakout_detected": False,
            "direction": "none",
            "entry_price": 0,
            "stop_loss": 0,
            "strength": 0,
            "consolidation_zones": [],
            "key_levels": {
                "support": [],
                "resistance": []
            }
        }

        try:
            # 데이터 로드
            df = self.get_ohlcv_data(timeframe, limit)

            if df.empty:
                self.logger.warning(f"{timeframe} 시간프레임 데이터 없음")
                return scan_result

            # 현재 가격
            current_price = df['close'].iloc[-1]

            # 1. 통합 구간 식별
            consolidation_zones = self.identify_consolidation_zones(
                df,
                self.config["consolidation_periods"]
            )

            # 2. 중요 지지/저항 레벨 식별
            support_levels, resistance_levels = self.find_key_levels(df)

            scan_result["key_levels"]["support"] = support_levels
            scan_result["key_levels"]["resistance"] = resistance_levels

            # 3. 각 통합 구간에서 돌파 확인
            for zone in consolidation_zones:
                breakout = self.detect_breakout(df, zone, self.config["confirmation_periods"])

                # 돌파가 감지되었으면 저장
                if breakout["breakout"]:
                    # ATR 계산 (손절 설정에 사용)
                    atr_value = df['atr'].iloc[-1]

                    # 손절 계산
                    if breakout["direction"] == "up":
                        stop_loss = breakout["price"] - (atr_value * self.config["sl_multiplier"])

                        # 지지 레벨 활용
                        for level in sorted(support_levels, reverse=True):
                            if level < breakout["price"]:
                                stop_loss = max(stop_loss, level * 0.99)
                                break
                    else:  # down
                        stop_loss = breakout["price"] + (atr_value * self.config["sl_multiplier"])

                        # 저항 레벨 활용
                        for level in sorted(resistance_levels):
                            if level > breakout["price"]:
                                stop_loss = min(stop_loss, level * 1.01)
                                break

                    scan_result.update({
                        "breakout_detected": True,
                        "direction": breakout["direction"],
                        "entry_price": breakout["price"],
                        "stop_loss": stop_loss,
                        "strength": breakout["strength"],
                        "consolidation_zones": consolidation_zones,
                        "atr_value": atr_value
                    })

                    # 첫 번째 유효한 돌파를 찾으면 종료
                    break

            self.logger.info(
                f"{timeframe} 스캔 완료: "
                f"돌파 감지={scan_result['breakout_detected']}, "
                f"방향={scan_result['direction']}, "
                f"강도={scan_result.get('strength', 0)}"
            )

        except Exception as e:
            self.logger.error(f"{timeframe} 돌파 스캔 중 오류 발생: {e}")

        return scan_result

    def calculate_take_profit_levels(
            self,
            entry_price: float,
            stop_loss: float,
            direction: str,
            key_levels: Dict[str, List[float]]
    ) -> List[Tuple[float, float]]:
        """
        이익 실현 레벨을 계산합니다.

        Args:
            entry_price: 진입 가격
            stop_loss: 손절 가격
            direction: 포지션 방향 ('up' 또는 'down')
            key_levels: 중요 지지/저항 레벨

        Returns:
            List[Tuple[float, float]]: (가격, 비율) 형태의 이익 실현 레벨 목록
        """
        tp_levels = []
        risk = abs(entry_price - stop_loss)

        if direction == "up":
            # 저항 레벨 확인
            resistance = sorted([r for r in key_levels.get("resistance", []) if r > entry_price])

            # 저항 레벨이 있으면 활용, 없으면 위험 배수 사용
            if resistance:
                # 가장 가까운 3개 저항 레벨 사용
                for i, level in enumerate(resistance[:3]):
                    portion = 0.3 if i < 2 else 0.4  # 마지막은 40%
                    tp_levels.append((level, portion))
            else:
                # 위험 배수 기반 TP 레벨
                for i, mult in enumerate(self.config["tp_multiplier"]):
                    tp_price = entry_price + (risk * mult)
                    portion = 0.3 if i < 2 else 0.4  # 마지막은 40%
                    tp_levels.append((tp_price, portion))
        else:  # direction == "down"
            # 지지 레벨 확인
            support = sorted([s for s in key_levels.get("support", []) if s < entry_price], reverse=True)

            # 지지 레벨이 있으면 활용, 없으면 위험 배수 사용
            if support:
                # 가장 가까운 3개 지지 레벨 사용
                for i, level in enumerate(support[:3]):
                    portion = 0.3 if i < 2 else 0.4  # 마지막은 40%
                    tp_levels.append((level, portion))
            else:
                # 위험 배수 기반 TP 레벨
                for i, mult in enumerate(self.config["tp_multiplier"]):
                    tp_price = entry_price - (risk * mult)
                    portion = 0.3 if i < 2 else 0.4  # 마지막은 40%
                    tp_levels.append((tp_price, portion))

        return tp_levels

    def calculate_signal(self) -> Dict[str, Any]:
        """
        돌파 전략 신호를 계산합니다.

        Returns:
            Dict: 신호 데이터
        """
        self.logger.info("돌파 전략 신호 계산 중...")

        # 기본 신호 데이터
        signal = {
            "timestamp": datetime.now().isoformat(),
            "symbol": self.symbol,
            "strategy": self.strategy_name,
            "signal_type": "none",
            "direction": "none",
            "entry_price": 0,
            "stop_loss": 0,
            "take_profit_levels": [],
            "strength": 0,
            "message": "No signal",
            "timeframe": "",
            "risk_reward_ratio": 0
        }

        try:
            # 각 시간프레임별 돌파 스캔
            tf_scans = {}
            for tf in self.config["timeframes"]:
                tf_scans[tf] = self.scan_for_breakouts(tf)

            # 시간프레임별 가중치 설정
            tf_weights = {
                "1d": 0.5,
                "4h": 0.3,
                "1h": 0.2,
                "15m": 0.1
            }

            # 전체 신호 강도 계산
            total_score = 0
            total_weight = 0

            # 돌파가 감지된 시간프레임 기록
            breakout_tfs = []

            for tf, scan in tf_scans.items():
                if scan["breakout_detected"]:
                    weight = tf_weights.get(tf, 0.1)
                    total_score += scan["strength"] * weight
                    total_weight += weight
                    breakout_tfs.append(tf)

            # 돌파가 없으면 기본값 반환
            if not breakout_tfs:
                return signal

            # 전체 신호 강도 정규화
            signal_strength = total_score / total_weight if total_weight > 0 else 0

            # 가장 중요한 시간프레임의 스캔 결과 사용
            for tf in self.config["timeframes"]:
                if tf in breakout_tfs:
                    primary_scan = tf_scans[tf]
                    break
            else:
                primary_scan = tf_scans[breakout_tfs[0]]

            # 진입 가격 및 손절 설정
            entry_price = primary_scan["entry_price"]
            stop_loss = primary_scan["stop_loss"]
            direction = primary_scan["direction"]

            # 이익 실현 레벨 계산
            tp_levels = self.calculate_take_profit_levels(
                entry_price,
                stop_loss,
                direction,
                primary_scan["key_levels"]
            )

            # 위험 대비 보상 비율 계산
            risk = abs(entry_price - stop_loss)
            highest_reward = abs(tp_levels[-1][0] - entry_price)
            risk_reward_ratio = highest_reward / risk if risk > 0 else 0

            # 신호 메시지 생성
            message = (
                f"{'롱' if direction == 'up' else '숏'} 돌파 신호: "
                f"{primary_scan['timeframe']} 시간프레임, "
                f"강도: {signal_strength:.1f}/100, "
                f"R:R = 1:{risk_reward_ratio:.1f}"
            )

            # 최종 신호 데이터
            signal.update({
                "signal_type": "entry",
                "direction": "long" if direction == "up" else "short",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit_levels": tp_levels,
                "strength": signal_strength,
                "message": message,
                "timeframe": primary_scan["timeframe"],
                "risk_reward_ratio": risk_reward_ratio,
                "atr_value": primary_scan.get("atr_value", 0),
                "breakout_tfs": breakout_tfs
            })

            # 신호 저장
            if signal["direction"] != "none":
                self.save_signal(
                    signal_type="entry",
                    direction=signal["direction"],
                    strength=signal["strength"],
                    price=signal["entry_price"],
                    timeframe=signal["timeframe"],
                    message=signal["message"],
                    entry_price=signal["entry_price"],
                    take_profit_levels=[tp[0] for tp in signal["take_profit_levels"]],
                    stop_loss_level=signal["stop_loss"]
                )

                self.logger.info(
                    f"돌파 전략 신호 생성: {signal['direction']}, 강도: {signal['strength']:.2f}%, R:R = 1:{signal['risk_reward_ratio']:.2f}")

        except Exception as e:
            self.logger.error(f"신호 계산 중 오류 발생: {e}")

        return signal
