# strategies/trend_following_strategy.py
"""
추세추종 전략

이 모듈은 추세추종 전략을 구현합니다.
중장기 추세를 식별하고 해당 방향으로 포지션을 진입하는 전략입니다.
"""

import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from strategies.base_strategy import BaseStrategy


class TrendFollowingStrategy(BaseStrategy):
    """
    추세추종 전략

    주요 개념:
    - 높은 시간프레임(HTF)에서 추세 방향 확인
    - 낮은 시간프레임(LTF)에서 진입 타이밍 식별
    - 추세 방향으로 포지션 진입
    - 이동평균, MACD, ADX 등의 지표를 활용하여 추세 강도와 방향 판단
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            config: Optional[Dict[str, Any]] = None
    ):
        """
        추세추종 전략 초기화

        Args:
            symbol: 거래 심볼
            config: 전략 설정
        """
        super().__init__("trend_following", "추세추종 전략", symbol)

        # 기본 설정
        self.default_config = {
            "htf_list": ["1w", "1d", "4h"],  # 추세 분석 시간프레임(높은 것부터)
            "ltf_list": ["1h", "15m"],  # 진입 분석 시간프레임
            "trend_ema_fast": 9,  # 빠른 EMA 기간
            "trend_ema_slow": 21,  # 느린 EMA 기간
            "trend_ema_base": 55,  # 기준 EMA 기간
            "trend_ema_long": 200,  # 장기 EMA 기간
            "adx_period": 14,  # ADX 기간
            "adx_threshold": 25,  # ADX 임계값 (추세 강도)
            "entry_rsi_period": 14,  # 진입 RSI 기간
            "entry_rsi_overbought": 70,  # RSI 과매수 임계값
            "entry_rsi_oversold": 30,  # RSI 과매도 임계값
            "minimum_swing": 0.5,  # 최소 스윙 크기 (%)
            "pullback_threshold": 0.3,  # 조정 임계값 (%)
            "tp_levels": [1.5, 2.5, 4.0],  # 이익 실현 레벨 (위험 배수)
            "sl_multiplier": 1.0,  # 손절 배수 (ATR 대비)
            "risk_per_trade": 0.01,  # 거래당 위험률 (계정의 %)
            "max_leverage": 10  # 최대 레버리지
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # ADX 계산 캐시
        self.adx_cache = {}

        # 추세 분석 결과 캐시
        self.trend_analysis = {}

        self.logger.info(f"추세추종 전략 설정됨: {self.config}")

    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        평균 방향성 지수(ADX)를 계산합니다.

        Args:
            df: OHLCV 데이터프레임
            period: ADX 계산 기간

        Returns:
            pd.Series: ADX 시리즈
        """
        # 캐시 키
        cache_key = f"{len(df)}_{period}"
        if cache_key in self.adx_cache:
            return self.adx_cache[cache_key]

        # True Range 계산
        df['tr0'] = abs(df['high'] - df['low'])
        df['tr1'] = abs(df['high'] - df['close'].shift())
        df['tr2'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)

        # +DM, -DM 계산
        df['up_move'] = df['high'] - df['high'].shift()
        df['down_move'] = df['low'].shift() - df['low']

        df['plus_dm'] = ((df['up_move'] > df['down_move']) & (df['up_move'] > 0)) * df['up_move']
        df['minus_dm'] = ((df['down_move'] > df['up_move']) & (df['down_move'] > 0)) * df['down_move']

        # +DI, -DI 계산
        df['tr' + str(period)] = df['tr'].rolling(window=period).sum()
        df['plus_di' + str(period)] = 100 * (df['plus_dm'].rolling(window=period).sum() / df['tr' + str(period)])
        df['minus_di' + str(period)] = 100 * (df['minus_dm'].rolling(window=period).sum() / df['tr' + str(period)])

        # ADX 계산
        df['dx'] = 100 * abs(df['plus_di' + str(period)] - df['minus_di' + str(period)]) / (
                df['plus_di' + str(period)] + df['minus_di' + str(period)])
        df['adx' + str(period)] = df['dx'].rolling(window=period).mean()

        # 캐시에 저장
        self.adx_cache[cache_key] = df['adx' + str(period)]

        return df['adx' + str(period)]

    def analyze_htf_trend(self) -> Dict[str, Any]:
        """
        높은 시간프레임(HTF)에서 추세를 분석합니다.

        Returns:
            Dict: 추세 분석 결과
        """
        self.logger.info("HTF 추세 분석 중...")

        # 추세 분석 결과 초기화
        trend_analysis = {
            "overall_trend": "neutral",
            "trend_strength": 0.0,
            "timeframes": {}
        }

        # 각 시간프레임 분석 가중치
        weights = {
            "1w": 0.4,
            "1d": 0.3,
            "4h": 0.2,
            "1h": 0.1
        }

        total_score = 0
        total_weight = 0

        # 각 시간프레임별 추세 분석
        for tf in self.config["htf_list"]:
            # 데이터 로드
            df = self.get_ohlcv_data(tf, 200)

            if df.empty:
                self.logger.warning(f"{tf} 시간프레임 데이터 없음, 건너뜀")
                continue

            # EMA 추세 확인
            ema_fast = df['ema' + str(self.config["trend_ema_fast"])]
            ema_slow = df['ema' + str(self.config["trend_ema_slow"])]
            ema_base = df['ema' + str(self.config["trend_ema_base"])]
            ema_long = df['ema' + str(self.config["trend_ema_long"])]

            # 데이터 유효성 검사
            if ema_fast.isna().iloc[-1] or ema_slow.isna().iloc[-1] or ema_base.isna().iloc[-1]:
                self.logger.warning(f"{tf} 시간프레임 EMA 데이터 불완전, 건너뜀")
                continue

            # 현재 가격
            current_price = df['close'].iloc[-1]

            # EMA 배열
            ema_trend_bullish = (
                    ema_fast.iloc[-1] > ema_slow.iloc[-1] > ema_base.iloc[-1] and
                    current_price > ema_fast.iloc[-1]
            )

            ema_trend_bearish = (
                    ema_fast.iloc[-1] < ema_slow.iloc[-1] < ema_base.iloc[-1] and
                    current_price < ema_fast.iloc[-1]
            )

            # ADX 계산 (추세 강도)
            adx = self.calculate_adx(df, self.config["adx_period"])
            adx_value = adx.iloc[-1]
            adx_rising = adx.iloc[-1] > adx.iloc[-5]  # 5개 캔들 전보다 상승 중인지

            # MACD 방향
            macd_line = df['macd_line'].iloc[-1]
            macd_signal = df['macd_signal'].iloc[-1]
            macd_hist = df['macd_histogram'].iloc[-1]
            macd_hist_prev = df['macd_histogram'].iloc[-2]

            macd_bullish = macd_line > macd_signal and macd_hist > 0 and macd_hist > macd_hist_prev
            macd_bearish = macd_line < macd_signal and macd_hist < 0 and macd_hist < macd_hist_prev

            # 추세 점수 계산 (-100 ~ +100)
            trend_score = 0

            # EMA 배열 기반 점수 (가장 중요)
            if ema_trend_bullish:
                trend_score += 50
            elif ema_trend_bearish:
                trend_score -= 50

            # 가격과 장기 EMA 비교
            if not ema_long.isna().iloc[-1]:
                if current_price > ema_long.iloc[-1]:
                    trend_score += 20
                else:
                    trend_score -= 20

            # ADX 강도 기반 점수
            if adx_value >= self.config["adx_threshold"]:
                if adx_rising:
                    trend_score += 10
                else:
                    trend_score += 5

            # MACD 기반 점수
            if macd_bullish:
                trend_score += 20
            elif macd_bearish:
                trend_score -= 20

            # 최종 추세 방향
            if trend_score >= 50:
                trend_direction = "bullish"
            elif trend_score <= -50:
                trend_direction = "bearish"
            else:
                trend_direction = "neutral"

            # 추세 강도 (0-100%)
            trend_strength = min(100, abs(trend_score))

            # 시간프레임별 결과 저장
            trend_analysis["timeframes"][tf] = {
                "trend": trend_direction,
                "strength": trend_strength,
                "score": trend_score,
                "adx": adx_value,
                "ema_status": "bullish" if ema_trend_bullish else "bearish" if ema_trend_bearish else "neutral",
                "macd_status": "bullish" if macd_bullish else "bearish" if macd_bearish else "neutral"
            }

            # 가중 평균에 추가
            tf_weight = weights.get(tf, 0.1)
            total_score += trend_score * tf_weight
            total_weight += tf_weight

        # 전체 추세 계산
        if total_weight > 0:
            avg_score = total_score / total_weight

            if avg_score >= 30:
                trend_analysis["overall_trend"] = "bullish"
            elif avg_score <= -30:
                trend_analysis["overall_trend"] = "bearish"
            else:
                trend_analysis["overall_trend"] = "neutral"

            trend_analysis["trend_strength"] = min(100, abs(avg_score))
            trend_analysis["trend_score"] = avg_score

        self.logger.info(
            f"HTF 추세 분석 완료: {trend_analysis['overall_trend']}, 강도: {trend_analysis['trend_strength']:.2f}%")

        # 결과 캐싱
        self.trend_analysis = trend_analysis

        return trend_analysis

    def find_ltf_entry(self, trend_direction: str) -> Dict[str, Any]:
        """
        낮은 시간프레임(LTF)에서 진입 포인트를 찾습니다.

        Args:
            trend_direction: 추세 방향 ('bullish' 또는 'bearish')

        Returns:
            Dict: 진입 분석 결과
        """
        self.logger.info(f"LTF 진입 포인트 분석 중: {trend_direction} 추세")

        # 기본 진입 결과
        entry_analysis = {
            "found_entry": False,
            "entry_type": None,
            "entry_timeframe": None,
            "entry_price": 0,
            "entry_strength": 0,
            "stop_loss": 0,
            "atr_value": 0
        }

        # 추세가 중립이면 진입 없음
        if trend_direction == "neutral":
            return entry_analysis

        # 각 LTF 시간프레임 분석
        for tf in self.config["ltf_list"]:
            # 데이터 로드
            df = self.get_ohlcv_data(tf, 100)

            if df.empty:
                self.logger.warning(f"{tf} 시간프레임 데이터 없음, 건너뜀")
                continue

            # 현재 가격
            current_price = df['close'].iloc[-1]

            # ATR 계산
            atr_value = df['atr'].iloc[-1]
            atr_percent = df['atr_percent'].iloc[-1]

            # 풀백/조정 진입 확인
            ema_fast = df['ema' + str(self.config["trend_ema_fast"])]
            ema_slow = df['ema' + str(self.config["trend_ema_slow"])]
            ema_base = df['ema' + str(self.config["trend_ema_base"])]

            # RSI 확인
            rsi = df['rsi']

            # 상승 추세에서 진입 조건
            if trend_direction == "bullish":
                # 1. 풀백 진입 전략: 가격이 빠른 EMA에 닿거나 약간 아래로 내려온 상태
                pullback_to_ema_fast = (
                        abs(current_price - ema_fast.iloc[-1]) / ema_fast.iloc[-1] < 0.01 or  # 1% 이내
                        (current_price < ema_fast.iloc[-1] and
                         (ema_fast.iloc[-1] - current_price) / ema_fast.iloc[-1] < self.config[
                             "pullback_threshold"] / 100)
                )

                # 2. 강세 구간에서 RSI 과매도 상태
                rsi_buy_signal = rsi.iloc[-1] < self.config["entry_rsi_oversold"]

                # 3. MACD 히스토그램 상승 반전
                macd_hist = df['macd_histogram']
                macd_turning_up = (
                        macd_hist.iloc[-1] > macd_hist.iloc[-2] > macd_hist.iloc[-3] and
                        macd_hist.iloc[-3] < 0
                )

                # 진입 스코어 계산 (복합 조건)
                entry_score = 0

                if pullback_to_ema_fast:
                    entry_score += 40
                    entry_type = "pullback_to_ema"

                if rsi_buy_signal:
                    entry_score += 30
                    entry_type = "rsi_oversold"

                if macd_turning_up:
                    entry_score += 30
                    entry_type = "macd_reversal"

                # 최소 하나 이상의 조건이 충족되고, 점수가 임계값 이상인 경우 진입
                if entry_score >= 30:
                    # 손절 계산: 베이스 EMA 아래 또는 ATR의 1-2배
                    if ema_base.iloc[-1] < current_price:
                        stop_loss = min(
                            ema_base.iloc[-1],
                            current_price - (atr_value * self.config["sl_multiplier"])
                        )
                    else:
                        stop_loss = current_price - (atr_value * self.config["sl_multiplier"])

                    # 스윙 크기 확인 (너무 작으면 진입하지 않음)
                    swing_percent = (current_price - stop_loss) / current_price * 100

                    if swing_percent >= self.config["minimum_swing"]:
                        entry_analysis = {
                            "found_entry": True,
                            "entry_type": entry_type,
                            "entry_timeframe": tf,
                            "entry_price": current_price,
                            "entry_strength": entry_score,
                            "stop_loss": stop_loss,
                            "atr_value": atr_value,
                            "direction": "long",
                            "swing_percent": swing_percent
                        }

                        self.logger.info(f"롱 진입 포인트 발견: {tf}, 유형: {entry_type}, 강도: {entry_score}/100")
                        break

            # 하락 추세에서 진입 조건
            elif trend_direction == "bearish":
                # 1. 풀백 진입 전략: 가격이 빠른 EMA에 닿거나 약간 위로 올라온 상태
                pullback_to_ema_fast = (
                        abs(current_price - ema_fast.iloc[-1]) / ema_fast.iloc[-1] < 0.01 or  # 1% 이내
                        (current_price > ema_fast.iloc[-1] and
                         (current_price - ema_fast.iloc[-1]) / ema_fast.iloc[-1] < self.config[
                             "pullback_threshold"] / 100)
                )

                # 2. 약세 구간에서 RSI 과매수 상태
                rsi_sell_signal = rsi.iloc[-1] > self.config["entry_rsi_overbought"]

                # 3. MACD 히스토그램 하락 반전
                macd_hist = df['macd_histogram']
                macd_turning_down = (
                        macd_hist.iloc[-1] < macd_hist.iloc[-2] < macd_hist.iloc[-3] and
                        macd_hist.iloc[-3] > 0
                )

                # 진입 스코어 계산 (복합 조건)
                entry_score = 0

                if pullback_to_ema_fast:
                    entry_score += 40
                    entry_type = "pullback_to_ema"

                if rsi_sell_signal:
                    entry_score += 30
                    entry_type = "rsi_overbought"

                if macd_turning_down:
                    entry_score += 30
                    entry_type = "macd_reversal"

                # 최소 하나 이상의 조건이 충족되고, 점수가 임계값 이상인 경우 진입
                if entry_score >= 30:
                    # 손절 계산: 베이스 EMA 위 또는 ATR의 1-2배
                    if ema_base.iloc[-1] > current_price:
                        stop_loss = max(
                            ema_base.iloc[-1],
                            current_price + (atr_value * self.config["sl_multiplier"])
                        )
                    else:
                        stop_loss = current_price + (atr_value * self.config["sl_multiplier"])

                    # 스윙 크기 확인 (너무 작으면 진입하지 않음)
                    swing_percent = (stop_loss - current_price) / current_price * 100

                    if swing_percent >= self.config["minimum_swing"]:
                        entry_analysis = {
                            "found_entry": True,
                            "entry_type": entry_type,
                            "entry_timeframe": tf,
                            "entry_price": current_price,
                            "entry_strength": entry_score,
                            "stop_loss": stop_loss,
                            "atr_value": atr_value,
                            "direction": "short",
                            "swing_percent": swing_percent
                        }

                        self.logger.info(f"숏 진입 포인트 발견: {tf}, 유형: {entry_type}, 강도: {entry_score}/100")
                        break

        return entry_analysis

    def calculate_take_profit_levels(
            self,
            entry_price: float,
            stop_loss: float,
            direction: str
    ) -> List[Tuple[float, float]]:
        """
        이익 실현 레벨을 계산합니다.

        Args:
            entry_price: 진입 가격
            stop_loss: 손절 가격
            direction: 포지션 방향 ('long' 또는 'short')

        Returns:
            List[Tuple[float, float]]: (가격, 비율) 형태의 이익 실현 레벨 목록
        """
        tp_levels = []

        # 위험 계산 (손절까지의 가격 차이)
        if direction == "long":
            risk = entry_price - stop_loss

            # 이익 실현 레벨 계산 (위험의 배수)
            for i, multiplier in enumerate(self.config["tp_levels"]):
                tp_price = entry_price + (risk * multiplier)
                # 포지션의 일부 비율 (첫 번째 TP에서 30%, 두 번째에서 30%, 마지막에서 40%)
                portion = 0.3 if i < len(self.config["tp_levels"]) - 1 else 0.4
                tp_levels.append((tp_price, portion))

        elif direction == "short":
            risk = stop_loss - entry_price

            # 이익 실현 레벨 계산 (위험의 배수)
            for i, multiplier in enumerate(self.config["tp_levels"]):
                tp_price = entry_price - (risk * multiplier)
                # 포지션의 일부 비율 (첫 번째 TP에서 30%, 두 번째에서 30%, 마지막에서 40%)
                portion = 0.3 if i < len(self.config["tp_levels"]) - 1 else 0.4
                tp_levels.append((tp_price, portion))

        return tp_levels

    def calculate_signal(self) -> Dict[str, Any]:
        """
        추세추종 전략 신호를 계산합니다.

        Returns:
            Dict: 신호 데이터
        """
        self.logger.info("추세추종 전략 신호 계산 중...")

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
            # 1. HTF 추세 분석
            trend_analysis = self.analyze_htf_trend()
            overall_trend = trend_analysis["overall_trend"]
            trend_strength = trend_analysis["trend_strength"]

            # 추세가 충분히 강하지 않으면 신호 없음
            if trend_strength < 50 or overall_trend == "neutral":
                return signal

            # 2. LTF 진입 포인트 분석
            entry_analysis = self.find_ltf_entry(overall_trend)

            # 진입 포인트를 찾지 못했으면 신호 없음
            if not entry_analysis["found_entry"]:
                return signal

            # 3. 이익 실현 레벨 계산
            tp_levels = self.calculate_take_profit_levels(
                entry_analysis["entry_price"],
                entry_analysis["stop_loss"],
                entry_analysis["direction"]
            )

            # 4. 위험 대비 보상 비율 계산
            risk = abs(entry_analysis["entry_price"] - entry_analysis["stop_loss"])
            highest_reward = abs(tp_levels[-1][0] - entry_analysis["entry_price"])
            risk_reward_ratio = highest_reward / risk if risk > 0 else 0

            # 5. 진입 신호 강도 계산 (추세 강도와 진입 강도 결합)
            signal_strength = (trend_strength * 0.7) + (entry_analysis["entry_strength"] * 0.3)

            # 6. 신호 메시지 생성
            message = (
                f"{entry_analysis['direction'].upper()} 진입 신호: "
                f"{entry_analysis['entry_type']} ({entry_analysis['entry_timeframe']}), "
                f"HTF 추세: {overall_trend.upper()} (강도: {trend_strength:.2f}%), "
                f"R:R = 1:{risk_reward_ratio:.2f}"
            )

            # 최종 신호 데이터
            signal.update({
                "signal_type": "entry",
                "direction": entry_analysis["direction"],
                "entry_price": entry_analysis["entry_price"],
                "suggested_stop_loss": entry_analysis["stop_loss"],
                "suggested_take_profit_levels": tp_levels,
                "strength": signal_strength,
                "message": message,
                "timeframe": entry_analysis["entry_timeframe"],
                "risk_reward_ratio": risk_reward_ratio,
                "atr_value": entry_analysis["atr_value"],
                "trend_strength": trend_strength,
                "entry_type": entry_analysis["entry_type"]
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
                    f"추세추종 전략 신호 생성: {signal['direction']}, 강도: {signal['strength']:.2f}%, R:R = 1:{signal['risk_reward_ratio']:.2f}")

        except Exception as e:
            self.logger.error(f"신호 계산 중 오류 발생: {e}")

        return signal
