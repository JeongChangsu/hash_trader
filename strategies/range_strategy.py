# strategies/range_strategy.py
"""
레인지 전략

이 모듈은 레인지 전략을 구현합니다.
가격이 특정 범위 내에서 움직일 때 범위의 경계에서 반대 방향으로 진입하는 전략입니다.
"""

import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from strategies.base_strategy import BaseStrategy


class RangeStrategy(BaseStrategy):
    """
    레인지 전략

    주요 개념:
    - 횡보장(레인지) 식별
    - 레인지 경계에서 반대 방향으로 진입
    - 과매수/과매도 지표를 활용한 확인
    - 볼륨 프로필을 통한 키 레벨 식별
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            config: Optional[Dict[str, Any]] = None
    ):
        """
        레인지 전략 초기화

        Args:
            symbol: 거래 심볼
            config: 전략 설정
        """
        super().__init__("range", "레인지 전략", symbol)

        # 기본 설정
        self.default_config = {
            "timeframes": ["4h", "1h", "15m"],  # 분석 시간프레임
            "min_range_periods": 20,  # 최소 레인지 기간
            "lookback_periods": 100,  # 분석 기간 (캔들 수)
            "min_range_width": 3.0,  # 최소 레인지 너비 (%)
            "max_range_width": 15.0,  # 최대 레인지 너비 (%)
            "min_touches": 3,  # 최소 터치 횟수
            "entry_buffer": 0.5,  # 진입 버퍼 (%)
            "rsi_period": 14,  # RSI 기간
            "rsi_overbought": 70,  # RSI 과매수 임계값
            "rsi_oversold": 30,  # RSI 과매도 임계값
            "bollinger_period": 20,  # 볼린저 밴드 기간
            "bollinger_dev": 2.0,  # 볼린저 밴드 표준편차
            "tp_pct": 80.0,  # 이익 실현 목표 (레인지의 %)
            "sl_buffer": 1.0,  # 손절 버퍼 (%)
            "risk_per_trade": 0.01,  # 거래당 위험률 (계정의 %)
            "max_leverage": 5  # 최대 레버리지
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # 레인지 상태 캐시
        self.range_state = {}

        self.logger.info(f"레인지 전략 설정됨: {self.config}")

    def detect_range(
            self,
            df: pd.DataFrame,
            min_periods: int = 20
    ) -> Dict[str, Any]:
        """
        가격 레인지를 감지합니다.

        Args:
            df: OHLCV 데이터프레임
            min_periods: 최소 레인지 기간

        Returns:
            Dict: 레인지 감지 결과
        """
        # 기본 결과
        result = {
            "is_range": False,
            "range_high": 0,
            "range_low": 0,
            "range_width_pct": 0,
            "upper_touches": 0,
            "lower_touches": 0,
            "range_periods": 0
        }

        if df.empty or len(df) < min_periods:
            return result

        # 최근 데이터 선택
        recent_df = df.iloc[-min_periods:]

        # 가격 범위 계산
        price_high = recent_df['high'].max()
        price_low = recent_df['low'].min()

        # 레인지 너비 계산 (%)
        range_width_pct = (price_high - price_low) / price_low * 100

        # 레인지 조건 확인
        if not (self.config["min_range_width"] <= range_width_pct <= self.config["max_range_width"]):
            return result

        # 상단/하단 터치 확인
        upper_touches = sum(1 for i in range(len(recent_df))
                            if abs(recent_df.iloc[i]['high'] - price_high) / price_high < 0.005)

        lower_touches = sum(1 for i in range(len(recent_df))
                            if abs(recent_df.iloc[i]['low'] - price_low) / price_low < 0.005)

        # 충분한 터치가 있는지 확인
        if upper_touches < self.config["min_touches"] or lower_touches < self.config["min_touches"]:
            return result

        # 추세 강도 확인 (너무 강한 추세면 레인지 아님)
        close_prices = recent_df['close'].values
        x = np.arange(len(close_prices))
        slope, _, r_value, _, _ = np.polyfit(x, close_prices, 1, full=True)[0:5]

        # 선형성(추세 강도) 확인
        r_squared = r_value ** 2
        normalized_slope = slope / close_prices.mean()

        # 강한 추세인 경우 레인지 아님
        if r_squared > 0.6 and abs(normalized_slope) > 0.001:
            return result

        # 레인지 확인됨
        result.update({
            "is_range": True,
            "range_high": price_high,
            "range_low": price_low,
            "range_width_pct": range_width_pct,
            "upper_touches": upper_touches,
            "lower_touches": lower_touches,
            "range_periods": min_periods,
            "r_squared": r_squared,
            "normalized_slope": normalized_slope
        })

        return result

    def identify_range_entries(
            self,
            df: pd.DataFrame,
            range_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        레인지 내 진입 포인트를 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            range_info: 레인지 정보

        Returns:
            Dict: 진입 포인트 정보
        """
        # 기본 결과
        result = {
            "entry_signal": "none",
            "entry_price": 0,
            "stop_loss": 0,
            "take_profit": 0,
            "signal_strength": 0
        }

        if not range_info["is_range"] or df.empty:
            return result

        # 레인지 경계
        range_high = range_info["range_high"]
        range_low = range_info["range_low"]

        # 현재 가격
        current_price = df['close'].iloc[-1]

        # RSI 확인
        rsi = df['rsi'].iloc[-1]

        # 볼린저 밴드 확인
        upper_band = df['upper_band'].iloc[-1]
        lower_band = df['lower_band'].iloc[-1]

        # ATR 계산 (손절 설정에 사용)
        atr_value = df['atr'].iloc[-1]

        # 진입 버퍼 계산
        entry_buffer_pct = self.config["entry_buffer"]
        high_buffer = range_high * (1 - entry_buffer_pct / 100)
        low_buffer = range_low * (1 + entry_buffer_pct / 100)

        # 신호 강도 계산 변수
        signal_strength = 0

        # 상단 근처에서 숏 진입 신호
        if current_price >= high_buffer:
            # 과매수 확인
            if rsi > self.config["rsi_overbought"]:
                signal_strength += 30

            # 볼린저 밴드 상단 확인
            if current_price > upper_band:
                signal_strength += 20

            # 거리 기반 점수
            distance_pct = (current_price - range_low) / range_low * 100
            normalized_distance = min(1, distance_pct / range_info["range_width_pct"])
            signal_strength += normalized_distance * 30

            # 터치 횟수 기반 점수
            touch_score = min(20, range_info["upper_touches"] * 5)
            signal_strength += touch_score

            if signal_strength >= 50:
                # 손절 계산: 레인지 상단 + 버퍼
                stop_loss = range_high * (1 + self.config["sl_buffer"] / 100)

                # 이익 실현 계산: 레인지 내부로 이동 (전체 너비의 80%)
                take_profit = range_high - (range_high - range_low) * (self.config["tp_pct"] / 100)

                result.update({
                    "entry_signal": "sell",
                    "entry_price": current_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "signal_strength": signal_strength,
                    "atr_value": atr_value
                })

        # 하단 근처에서 롱 진입 신호
        elif current_price <= low_buffer:
            # 과매도 확인
            if rsi < self.config["rsi_oversold"]:
                signal_strength += 30

            # 볼린저 밴드 하단 확인
            if current_price < lower_band:
                signal_strength += 20

            # 거리 기반 점수
            distance_pct = (range_high - current_price) / current_price * 100
            normalized_distance = min(1, distance_pct / range_info["range_width_pct"])
            signal_strength += normalized_distance * 30

            # 터치 횟수 기반 점수
            touch_score = min(20, range_info["lower_touches"] * 5)
            signal_strength += touch_score

            if signal_strength >= 50:
                # 손절 계산: 레인지 하단 - 버퍼
                stop_loss = range_low * (1 - self.config["sl_buffer"] / 100)

                # 이익 실현 계산: 레인지 내부로 이동 (전체 너비의 80%)
                take_profit = range_low + (range_high - range_low) * (self.config["tp_pct"] / 100)

                result.update({
                    "entry_signal": "buy",
                    "entry_price": current_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "signal_strength": signal_strength,
                    "atr_value": atr_value
                })

        return result

    def analyze_range_market(
            self,
            timeframe: str,
            limit: int = 100
    ) -> Dict[str, Any]:
        """
        특정 시간프레임에서 레인지 시장을 분석합니다.

        Args:
            timeframe: 시간프레임
            limit: 분석할 캔들 수

        Returns:
            Dict: 레인지 분석 결과
        """
        self.logger.info(f"{timeframe} 시간프레임 레인지 분석 중...")

        # 분석 결과 초기화
        range_analysis = {
            "timeframe": timeframe,
            "is_range_market": False,
            "range_info": {},
            "entry_signal": "none",
            "entry_price": 0,
            "stop_loss": 0,
            "take_profit": 0,
            "signal_strength": 0
        }

        try:
            # 데이터 로드
            df = self.get_ohlcv_data(timeframe, limit)

            if df.empty:
                self.logger.warning(f"{timeframe} 시간프레임 데이터 없음")
                return range_analysis

            # 레인지 감지
            range_info = self.detect_range(
                df,
                self.config["min_range_periods"]
            )

            range_analysis["is_range_market"] = range_info["is_range"]
            range_analysis["range_info"] = range_info

            if not range_info["is_range"]:
                self.logger.info(f"{timeframe} 레인지 시장 아님")
                return range_analysis

            # 레인지 진입 포인트 식별
            entry_info = self.identify_range_entries(df, range_info)

            range_analysis.update({
                "entry_signal": entry_info["entry_signal"],
                "entry_price": entry_info["entry_price"],
                "stop_loss": entry_info["stop_loss"],
                "take_profit": entry_info["take_profit"],
                "signal_strength": entry_info["signal_strength"],
                "atr_value": entry_info.get("atr_value", 0)
            })

            self.logger.info(
                f"{timeframe} 레인지 분석 완료: "
                f"레인지 시장={range_info['is_range']}, "
                f"진입 신호={entry_info['entry_signal']}, "
                f"신호 강도={entry_info['signal_strength']}"
            )

        except Exception as e:
            self.logger.error(f"{timeframe} 레인지 분석 중 오류 발생: {e}")

        return range_analysis

    def calculate_signal(self) -> Dict[str, Any]:
        """
        레인지 전략 신호를 계산합니다.

        Returns:
            Dict: 신호 데이터
        """
        self.logger.info("레인지 전략 신호 계산 중...")

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
            # 각 시간프레임별 레인지 분석
            tf_analysis = {}
            for tf in self.config["timeframes"]:
                tf_analysis[tf] = self.analyze_range_market(tf)

            # 시간프레임별 가중치 설정
            tf_weights = {
                "4h": 0.5,
                "1h": 0.3,
                "15m": 0.2
            }

            # 신호 강도 계산을 위한 변수
            total_score = 0
            total_weight = 0
            valid_signal_tfs = []

            # 유효한 신호가 있는 시간프레임 찾기
            for tf, analysis in tf_analysis.items():
                if analysis["is_range_market"] and analysis["entry_signal"] != "none":
                    weight = tf_weights.get(tf, 0.1)
                    total_score += analysis["signal_strength"] * weight
                    total_weight += weight
                    valid_signal_tfs.append(tf)

            # 유효한 신호가 없으면 기본값 반환
            if not valid_signal_tfs:
                return signal

            # 신호 강도 계산
            signal_strength = total_score / total_weight if total_weight > 0 else 0

            # 가장 중요한 시간프레임의 신호 사용
            primary_tf = valid_signal_tfs[0]  # 이미 가중치 높은 순으로 정렬됨
            primary_analysis = tf_analysis[primary_tf]

            # 진입 방향
            entry_signal = primary_analysis["entry_signal"]
            entry_price = primary_analysis["entry_price"]
            stop_loss = primary_analysis["stop_loss"]
            take_profit = primary_analysis["take_profit"]

            # 위험 대비 보상 비율 계산
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            risk_reward_ratio = reward / risk if risk > 0 else 0

            # 이익 실현 레벨 (하나만 있음)
            tp_levels = [(take_profit, 1.0)]  # 100% 물량을 한 번에 처리

            # 신호 메시지 생성
            range_width = primary_analysis["range_info"]["range_width_pct"]
            upper_touches = primary_analysis["range_info"]["upper_touches"]
            lower_touches = primary_analysis["range_info"]["lower_touches"]

            message = (
                f"{'롱' if entry_signal == 'buy' else '숏'} 레인지 신호: "
                f"{primary_tf} 시간프레임, "
                f"레인지 너비: {range_width:.2f}%, "
                f"터치 횟수: 상단 {upper_touches}회, 하단 {lower_touches}회, "
                f"R:R = 1:{risk_reward_ratio:.1f}"
            )

            # 최종 신호 데이터
            signal.update({
                "signal_type": "entry",
                "direction": "long" if entry_signal == "buy" else "short",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit_levels": tp_levels,
                "strength": signal_strength,
                "message": message,
                "timeframe": primary_tf,
                "risk_reward_ratio": risk_reward_ratio,
                "range_info": primary_analysis["range_info"],
                "atr_value": primary_analysis.get("atr_value", 0)
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
                    f"레인지 전략 신호 생성: {signal['direction']}, 강도: {signal['strength']:.2f}%, R:R = 1:{signal['risk_reward_ratio']:.2f}")

        except Exception as e:
            self.logger.error(f"신호 계산 중 오류 발생: {e}")

        return signal
