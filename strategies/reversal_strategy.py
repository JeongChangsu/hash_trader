# strategies/reversal_strategy.py
"""
반전 전략

이 모듈은 추세 반전 전략을 구현합니다.
과매수/과매도 상태에서 추세 반전을 예측하여 역방향으로 포지션을 잡는 전략입니다.
"""

import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from strategies.base_strategy import BaseStrategy


class ReversalStrategy(BaseStrategy):
    """
    반전 전략

    주요 개념:
    - 과매수/과매도 상태 식별
    - 다이버전스(Divergence) 패턴 검출
    - 추세 약화 신호 포착
    - 차트 패턴(이중 바닥/이중 정상 등) 식별
    - 주요 지지/저항 레벨에서의 반전 신호 확인
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            config: Optional[Dict[str, Any]] = None
    ):
        """
        반전 전략 초기화

        Args:
            symbol: 거래 심볼
            config: 전략 설정
        """
        super().__init__("reversal", "반전 전략", symbol)

        # 기본 설정
        self.default_config = {
            "timeframes": ["1d", "4h", "1h"],  # 분석 시간프레임
            "rsi_period": 14,  # RSI 기간
            "rsi_extreme_overbought": 75,  # 극단적 과매수 임계값
            "rsi_extreme_oversold": 25,  # 극단적 과매도 임계값
            "rsi_overbought": 70,  # 과매수 임계값
            "rsi_oversold": 30,  # 과매도 임계값
            "stoch_period": 14,  # 스토캐스틱 기간
            "stoch_extreme": 15,  # 스토캐스틱 극단값
            "macd_fast": 12,  # MACD 빠른 기간
            "macd_slow": 26,  # MACD 느린 기간
            "macd_signal": 9,  # MACD 시그널 기간
            "ema_fast": 8,  # 빠른 EMA 기간
            "ema_slow": 21,  # 느린 EMA 기간
            "bollinger_period": 20,  # 볼린저 밴드 기간
            "bollinger_std": 2,  # 볼린저 밴드 표준편차
            "min_reversal_score": 65,  # 최소 반전 점수
            "divergence_lookback": 20,  # 다이버전스 확인 기간
            "tp_multiplier": 1.5,  # 이익 실현 배수
            "sl_multiplier": 1.0,  # 손절 배수
            "risk_per_trade": 0.01  # 거래당 위험률 (계정의 %)
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # 차트 패턴 분석 결과 캐시
        self.patterns_cache = {}

        # 지지/저항 레벨 캐시
        self.sr_levels_cache = {}

        self.logger.info(f"반전 전략 설정됨: {self.config}")

    def detect_divergence(
            self,
            df: pd.DataFrame,
            lookback: int = 20,
            indicator: str = 'rsi'
    ) -> Dict[str, Any]:
        """
        가격과 지표 간의 다이버전스를 감지합니다.

        Args:
            df: OHLCV 데이터프레임
            lookback: 탐색 기간
            indicator: 사용할 지표 ('rsi' 또는 'macd')

        Returns:
            Dict: 다이버전스 감지 결과
        """
        result = {
            "bullish_divergence": False,
            "bearish_divergence": False,
            "price_low_idx": None,
            "price_high_idx": None,
            "indicator_low_idx": None,
            "indicator_high_idx": None
        }

        if df.empty or len(df) < lookback:
            return result

        # 최근 N개 캔들 선택
        recent_df = df.iloc[-lookback:]

        # 지표 선택
        if indicator == 'rsi':
            ind_values = recent_df['rsi']
        elif indicator == 'macd':
            ind_values = recent_df['macd_line']
        else:
            return result

        # 가격 저점/고점 찾기
        price_lows_idx = recent_df['low'].rolling(window=5, center=True).apply(
            lambda x: np.argmin(x) == 2 and np.min(x) == x.iloc[2], raw=False
        )

        price_highs_idx = recent_df['high'].rolling(window=5, center=True).apply(
            lambda x: np.argmax(x) == 2 and np.max(x) == x.iloc[2], raw=False
        )

        # 지표 저점/고점 찾기
        ind_lows_idx = ind_values.rolling(window=5, center=True).apply(
            lambda x: np.argmin(x) == 2 and np.min(x) == x.iloc[2], raw=False
        )

        ind_highs_idx = ind_values.rolling(window=5, center=True).apply(
            lambda x: np.argmax(x) == 2 and np.max(x) == x.iloc[2], raw=False
        )

        # 가격 저점/고점 인덱스
        price_low_idx = price_lows_idx[price_lows_idx].index.tolist()
        price_high_idx = price_highs_idx[price_highs_idx].index.tolist()

        # 지표 저점/고점 인덱스
        ind_low_idx = ind_lows_idx[ind_lows_idx].index.tolist()
        ind_high_idx = ind_highs_idx[ind_highs_idx].index.tolist()

        # 최소 2개의 저점과 고점이 필요
        if len(price_low_idx) < 2 or len(price_high_idx) < 2 or len(ind_low_idx) < 2 or len(ind_high_idx) < 2:
            return result

        # 최근 두 개의 가격 저점/고점
        last_price_lows = recent_df.loc[price_low_idx[-2:]]['low'].values
        last_price_highs = recent_df.loc[price_high_idx[-2:]]['high'].values

        # 최근 두 개의 지표 저점/고점
        if indicator == 'rsi':
            last_ind_lows = recent_df.loc[ind_low_idx[-2:]]['rsi'].values
            last_ind_highs = recent_df.loc[ind_high_idx[-2:]]['rsi'].values
        else:  # macd
            last_ind_lows = recent_df.loc[ind_low_idx[-2:]]['macd_line'].values
            last_ind_highs = recent_df.loc[ind_high_idx[-2:]]['macd_line'].values

        # 강세 다이버전스 확인 (가격은 하락하는데 지표는 상승)
        if last_price_lows[0] > last_price_lows[1] and last_ind_lows[0] < last_ind_lows[1]:
            result["bullish_divergence"] = True
            result["price_low_idx"] = price_low_idx[-2:]
            result["indicator_low_idx"] = ind_low_idx[-2:]

        # 약세 다이버전스 확인 (가격은 상승하는데 지표는 하락)
        if last_price_highs[0] < last_price_highs[1] and last_ind_highs[0] > last_ind_highs[1]:
            result["bearish_divergence"] = True
            result["price_high_idx"] = price_high_idx[-2:]
            result["indicator_high_idx"] = ind_high_idx[-2:]

        return result

    def check_overbought_oversold(self, df: pd.DataFrame) -> Dict[str, bool]:
        """
        과매수/과매도 상태를 확인합니다.

        Args:
            df: OHLCV 데이터프레임

        Returns:
            Dict: 과매수/과매도 상태
        """
        result = {
            "extreme_overbought": False,
            "overbought": False,
            "extreme_oversold": False,
            "oversold": False
        }

        if df.empty or 'rsi' not in df.columns or 'stoch_k' not in df.columns:
            return result

        # 가장 최근 값
        last_rsi = df['rsi'].iloc[-1]
        last_stoch_k = df['stoch_k'].iloc[-1]
        last_stoch_d = df['stoch_d'].iloc[-1]

        # 볼린저 밴드 관련 계산
        last_close = df['close'].iloc[-1]
        last_upper_band = df['upper_band'].iloc[-1]
        last_lower_band = df['lower_band'].iloc[-1]

        # 과매수 확인
        if last_rsi > self.config["rsi_extreme_overbought"] and last_stoch_k > 80 and last_stoch_d > 80:
            result["extreme_overbought"] = True
        elif last_rsi > self.config["rsi_overbought"] or (last_stoch_k > 80 and last_close > last_upper_band):
            result["overbought"] = True

        # 과매도 확인
        if last_rsi < self.config["rsi_extreme_oversold"] and last_stoch_k < 20 and last_stoch_d < 20:
            result["extreme_oversold"] = True
        elif last_rsi < self.config["rsi_oversold"] or (last_stoch_k < 20 and last_close < last_lower_band):
            result["oversold"] = True

        return result

    def detect_failed_swing(self, df: pd.DataFrame) -> Dict[str, bool]:
        """
        실패한 스윙을 감지합니다 (추세 약화 신호).

        Args:
            df: OHLCV 데이터프레임

        Returns:
            Dict: 실패한 스윙 감지 결과
        """
        result = {
            "failed_swing_high": False,
            "failed_swing_low": False
        }

        if df.empty or len(df) < 10:
            return result

        # 최근 10개 캔들
        recent_df = df.iloc[-10:]

        # 실패한 스윙 고점: 가격이 이전 고점을 뚫지 못하고 하락
        if recent_df['high'].iloc[-3] > recent_df['high'].iloc[-4] > recent_df['high'].iloc[-5] and \
                recent_df['high'].iloc[-2] < recent_df['high'].iloc[-3] and \
                recent_df['high'].iloc[-1] < recent_df['high'].iloc[-2]:
            result["failed_swing_high"] = True

        # 실패한 스윙 저점: 가격이 이전 저점을 뚫지 못하고 상승
        if recent_df['low'].iloc[-3] < recent_df['low'].iloc[-4] < recent_df['low'].iloc[-5] and \
                recent_df['low'].iloc[-2] > recent_df['low'].iloc[-3] and \
                recent_df['low'].iloc[-1] > recent_df['low'].iloc[-2]:
            result["failed_swing_low"] = True

        return result

    def check_key_level_rejection(
            self,
            df: pd.DataFrame,
            support_resistance: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        주요 지지/저항 레벨에서의 거부(rejection) 현상을 확인합니다.

        Args:
            df: OHLCV 데이터프레임
            support_resistance: 지지/저항 레벨 정보

        Returns:
            Dict: 거부 현상 확인 결과
        """
        result = {
            "support_rejection": False,
            "resistance_rejection": False,
            "level_price": 0,
            "level_type": ""
        }

        if df.empty or not support_resistance:
            return result

        # 현재 가격
        current_price = df['close'].iloc[-1]
        current_low = df['low'].iloc[-1]
        current_high = df['high'].iloc[-1]

        # 주요 지지 레벨 확인
        for level in support_resistance.get('support', []):
            level_price = level.get('price', 0)

            # 지지 레벨 근처에서의 거부(하락 후 반등)
            if abs(current_low - level_price) / level_price < 0.01 and current_price > current_low:
                result["support_rejection"] = True
                result["level_price"] = level_price
                result["level_type"] = "support"
                break

        # 주요 저항 레벨 확인
        for level in support_resistance.get('resistance', []):
            level_price = level.get('price', 0)

            # 저항 레벨 근처에서의 거부(상승 후 하락)
            if abs(current_high - level_price) / level_price < 0.01 and current_price < current_high:
                result["resistance_rejection"] = True
                result["level_price"] = level_price
                result["level_type"] = "resistance"
                break

        return result

    def analyze_reversal_signals(
            self,
            timeframe: str,
            limit: int = 100
    ) -> Dict[str, Any]:
        """
        특정 시간프레임에서 반전 신호를 분석합니다.

        Args:
            timeframe: 시간프레임
            limit: 분석할 캔들 수

        Returns:
            Dict: 반전 신호 분석 결과
        """
        self.logger.info(f"{timeframe} 시간프레임 반전 신호 분석 중...")

        # 분석 결과 초기화
        reversal_analysis = {
            "timeframe": timeframe,
            "bullish_score": 0,
            "bearish_score": 0,
            "signals": {
                "bullish": [],
                "bearish": []
            },
            "recommendation": "none",
            "entry_price": 0,
            "stop_loss": 0
        }

        try:
            # 데이터 로드
            df = self.get_ohlcv_data(timeframe, limit)

            if df.empty:
                self.logger.warning(f"{timeframe} 시간프레임 데이터 없음")
                return reversal_analysis

            # 현재 가격
            current_price = df['close'].iloc[-1]

            # 1. 과매수/과매도 확인
            ob_os_status = self.check_overbought_oversold(df)

            # 2. 다이버전스 감지
            rsi_divergence = self.detect_divergence(df, self.config["divergence_lookback"], 'rsi')
            macd_divergence = self.detect_divergence(df, self.config["divergence_lookback"], 'macd')

            # 3. 실패한 스윙 감지
            failed_swing = self.detect_failed_swing(df)

            # 4. 지지/저항 레벨 가져오기
            support_resistance = self.get_support_resistance_levels()

            # 5. 레벨 거부 확인
            level_rejection = self.check_key_level_rejection(df, support_resistance)

            # 6. 볼린저 밴드 터치 확인
            bb_touch_lower = df['low'].iloc[-1] <= df['lower_band'].iloc[-1]
            bb_touch_upper = df['high'].iloc[-1] >= df['upper_band'].iloc[-1]

            # 7. 이동평균 방향 확인
            ema_direction = "neutral"
            if 'ema' + str(self.config["ema_fast"]) in df.columns and 'ema' + str(
                    self.config["ema_slow"]) in df.columns:
                ema_fast = df['ema' + str(self.config["ema_fast"])].iloc[-1]
                ema_slow = df['ema' + str(self.config["ema_slow"])].iloc[-1]

                if ema_fast > ema_slow and ema_fast > ema_fast.iloc[-5]:  # 5개 캔들 전과 비교
                    ema_direction = "up"
                elif ema_fast < ema_slow and ema_fast < ema_fast.iloc[-5]:
                    ema_direction = "down"

            # 8. ATR 계산 (변동성 측정)
            atr_value = df['atr'].iloc[-1]

            # ====== 강세 반전 신호 분석 ======
            bullish_score = 0
            bullish_signals = []

            # 과매도 상태
            if ob_os_status["extreme_oversold"]:
                bullish_score += 30
                bullish_signals.append("극단적 과매도 상태")
            elif ob_os_status["oversold"]:
                bullish_score += 20
                bullish_signals.append("과매도 상태")

            # RSI 다이버전스
            if rsi_divergence["bullish_divergence"]:
                bullish_score += 25
                bullish_signals.append("RSI 강세 다이버전스")

            # MACD 다이버전스
            if macd_divergence["bullish_divergence"]:
                bullish_score += 20
                bullish_signals.append("MACD 강세 다이버전스")

            # 실패한 스윙 저점
            if failed_swing["failed_swing_low"]:
                bullish_score += 15
                bullish_signals.append("실패한 스윙 저점")

            # 지지 레벨 거부
            if level_rejection["support_rejection"]:
                bullish_score += 20
                bullish_signals.append(f"지지 레벨 거부 (가격: {level_rejection['level_price']})")

            # 볼린저 밴드 하단 터치
            if bb_touch_lower:
                bullish_score += 15
                bullish_signals.append("볼린저 밴드 하단 터치")

            # ====== 약세 반전 신호 분석 ======
            bearish_score = 0
            bearish_signals = []

            # 과매수 상태
            if ob_os_status["extreme_overbought"]:
                bearish_score += 30
                bearish_signals.append("극단적 과매수 상태")
            elif ob_os_status["overbought"]:
                bearish_score += 20
                bearish_signals.append("과매수 상태")

            # RSI 다이버전스
            if rsi_divergence["bearish_divergence"]:
                bearish_score += 25
                bearish_signals.append("RSI 약세 다이버전스")

            # MACD 다이버전스
            if macd_divergence["bearish_divergence"]:
                bearish_score += 20
                bearish_signals.append("MACD 약세 다이버전스")

            # 실패한 스윙 고점
            if failed_swing["failed_swing_high"]:
                bearish_score += 15
                bearish_signals.append("실패한 스윙 고점")

            # 저항 레벨 거부
            if level_rejection["resistance_rejection"]:
                bearish_score += 20
                bearish_signals.append(f"저항 레벨 거부 (가격: {level_rejection['level_price']})")

            # 볼린저 밴드 상단 터치
            if bb_touch_upper:
                bearish_score += 15
                bearish_signals.append("볼린저 밴드 상단 터치")

            # 현재 이동평균 방향에 따른 점수 조정
            if ema_direction == "up":
                bearish_score += 10  # 상승 중인 MA는 하락 반전 가능성 약간 높임
                bullish_score -= 10  # 상승 중인 MA는 상승 반전 가능성 감소
            elif ema_direction == "down":
                bullish_score += 10  # 하락 중인 MA는 상승 반전 가능성 약간 높임
                bearish_score -= 10  # 하락 중인 MA는 하락 반전 가능성 감소

            # 최종 점수 정규화 (0-100)
            bullish_score = max(0, min(100, bullish_score))
            bearish_score = max(0, min(100, bearish_score))

            # 최종 추천
            recommendation = "none"
            entry_price = current_price
            stop_loss = 0

            # 강세 반전 신호가 더 강한 경우 (롱 진입)
            if bullish_score > self.config["min_reversal_score"] and bullish_score > bearish_score:
                recommendation = "buy"
                # 손절: 현재 가격에서 ATR의 1배
                stop_loss = current_price - (atr_value * self.config["sl_multiplier"])

                # 지지 레벨이 있다면 그 값도 고려
                if level_rejection["support_rejection"]:
                    stop_loss = min(stop_loss, level_rejection["level_price"] * 0.99)

            # 약세 반전 신호가 더 강한 경우 (숏 진입)
            elif bearish_score > self.config["min_reversal_score"] and bearish_score > bullish_score:
                recommendation = "sell"
                # 손절: 현재 가격에서 ATR의 1배
                stop_loss = current_price + (atr_value * self.config["sl_multiplier"])

                # 저항 레벨이 있다면 그 값도 고려
                if level_rejection["resistance_rejection"]:
                    stop_loss = max(stop_loss, level_rejection["level_price"] * 1.01)

            # 분석 결과 저장
            reversal_analysis.update({
                "bullish_score": bullish_score,
                "bearish_score": bearish_score,
                "signals": {
                    "bullish": bullish_signals,
                    "bearish": bearish_signals
                },
                "recommendation": recommendation,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "atr_value": atr_value
            })

            self.logger.info(
                f"{timeframe} 분석 완료: "
                f"강세 점수={bullish_score}, 약세 점수={bearish_score}, "
                f"추천={recommendation}"
            )

        except Exception as e:
            self.logger.error(f"{timeframe} 반전 신호 분석 중 오류 발생: {e}")

        return reversal_analysis

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
            direction: 포지션 방향 ('buy' 또는 'sell')

        Returns:
            List[Tuple[float, float]]: (가격, 비율) 형태의 이익 실현 레벨 목록
        """
        tp_levels = []
        risk = abs(entry_price - stop_loss)

        if direction == "buy":
            # 롱 포지션 이익 실현 (위험의 1.5배, 2.5배, 4배)
            tp1 = entry_price + (risk * 1.5)
            tp2 = entry_price + (risk * 2.5)
            tp3 = entry_price + (risk * 4.0)

            tp_levels = [
                (tp1, 0.3),  # 30% 물량
                (tp2, 0.3),  # 30% 물량
                (tp3, 0.4)  # 40% 물량
            ]
        else:  # direction == "sell"
            # 숏 포지션 이익 실현
            tp1 = entry_price - (risk * 1.5)
            tp2 = entry_price - (risk * 2.5)
            tp3 = entry_price - (risk * 4.0)

            tp_levels = [
                (tp1, 0.3),  # 30% 물량
                (tp2, 0.3),  # 30% 물량
                (tp3, 0.4)  # 40% 물량
            ]

        return tp_levels

    def calculate_signal(self) -> Dict[str, Any]:
        """
        반전 전략 신호를 계산합니다.

        Returns:
            Dict: 신호 데이터
        """
        self.logger.info("반전 전략 신호 계산 중...")

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
            # 각 시간프레임별 반전 신호 분석
            tf_signals = {}
            for tf in self.config["timeframes"]:
                tf_signals[tf] = self.analyze_reversal_signals(tf)

            # 시간프레임별 가중치 설정
            tf_weights = {
                "1d": 0.5,
                "4h": 0.3,
                "1h": 0.2,
                "15m": 0.1
            }

            # 전체 신호 강도 계산
            bullish_score = 0
            bearish_score = 0
            total_weight = 0

            for tf, analysis in tf_signals.items():
                weight = tf_weights.get(tf, 0.1)
                bullish_score += analysis["bullish_score"] * weight
                bearish_score += analysis["bearish_score"] * weight
                total_weight += weight

            if total_weight > 0:
                bullish_score /= total_weight
                bearish_score /= total_weight

            # 신호 방향 결정
            signal_direction = "none"
            signal_timeframe = ""
            entry_price = 0
            stop_loss = 0

            # 강세 반전 신호가 더 강하고 임계값 이상인 경우 (롱 진입)
            if bullish_score > self.config["min_reversal_score"] and bullish_score > bearish_score:
                signal_direction = "buy"

                # 가장 강한 신호를 보이는 시간프레임 찾기
                strongest_tf = max(tf_signals.items(), key=lambda x: x[1]["bullish_score"])
                signal_timeframe = strongest_tf[0]
                entry_price = strongest_tf[1]["entry_price"]
                stop_loss = strongest_tf[1]["stop_loss"]

            # 약세 반전 신호가 더 강하고 임계값 이상인 경우 (숏 진입)
            elif bearish_score > self.config["min_reversal_score"] and bearish_score > bullish_score:
                signal_direction = "sell"

                # 가장 강한 신호를 보이는 시간프레임 찾기
                strongest_tf = max(tf_signals.items(), key=lambda x: x[1]["bearish_score"])
                signal_timeframe = strongest_tf[0]
                entry_price = strongest_tf[1]["entry_price"]
                stop_loss = strongest_tf[1]["stop_loss"]

            # 신호가 없으면 기본값 반환
            if signal_direction == "none":
                return signal

            # 이익 실현 레벨 계산
            tp_levels = self.calculate_take_profit_levels(entry_price, stop_loss, signal_direction)

            # 위험 대비 보상 비율 계산
            risk = abs(entry_price - stop_loss)
            highest_reward = abs(tp_levels[-1][0] - entry_price)
            risk_reward_ratio = highest_reward / risk if risk > 0 else 0

            # 신호 강도 계산
            signal_strength = bullish_score if signal_direction == "buy" else bearish_score

            # 신호 메시지 생성
            signals_list = tf_signals[signal_timeframe]["signals"][
                "bullish" if signal_direction == "buy" else "bearish"]

            signals_str = ", ".join(signals_list[:3])  # 상위 3개 신호만 포함
            message = (
                f"{'롱' if signal_direction == 'buy' else '숏'} 반전 신호: "
                f"{signals_str} ({signal_timeframe}), "
                f"점수: {signal_strength:.1f}/100, "
                f"R:R = 1:{risk_reward_ratio:.1f}"
            )

            # 최종 신호 데이터
            signal.update({
                "signal_type": "entry",
                "direction": "long" if signal_direction == "buy" else "short",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit_levels": tp_levels,
                "strength": signal_strength,
                "message": message,
                "timeframe": signal_timeframe,
                "risk_reward_ratio": risk_reward_ratio,
                "atr_value": tf_signals[signal_timeframe]["atr_value"],
                "signals": signals_list
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
                    f"반전 전략 신호 생성: {signal['direction']}, 강도: {signal['strength']:.2f}%, R:R = 1:{signal['risk_reward_ratio']:.2f}")

        except Exception as e:
            self.logger.error(f"신호 계산 중 오류 발생: {e}")

        return signal
