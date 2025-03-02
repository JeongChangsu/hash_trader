# analyzers/chart_pattern_analyzer.py
"""
차트 패턴 분석기

이 모듈은 OHLCV 데이터를 분석하여 차트 패턴을 식별합니다.
두 바닥, 삼각형, 헤드앤숄더 등 다양한 패턴을 자동으로 검출하고,
지지/저항 레벨과 추세선을 식별합니다.
"""

import logging
import numpy as np
import pandas as pd
from scipy.signal import find_peaks, peak_prominences
from typing import Dict, List, Tuple, Optional, Any, Union
import psycopg2
import json
import redis
import time
from datetime import datetime, timedelta

from config.settings import POSTGRES_CONFIG, REDIS_CONFIG
from config.logging_config import configure_logging


class ChartPatternAnalyzer:
    """
    차트 패턴을 식별하고 분석하는 클래스
    """

    def __init__(self):
        """ChartPatternAnalyzer 초기화"""
        self.logger = configure_logging("chart_pattern_analyzer")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 패턴 유효성 확인을 위한 최소 바 수
        self.min_pattern_bars = 5

        # 패턴 유효성 확인을 위한 최소 변동성 (%)
        self.min_volatility_pct = 1.0

        # 피크 감지를 위한 최소 두드러짐
        self.peak_prominence = 0.005  # 0.5%

        # 피크 감지를 위한 최소 거리
        self.peak_distance = 5  # 5개 캔들

        # 지지/저항 레벨 식별을 위한 터치 횟수 임계값
        self.support_resistance_touches = 3

        # 추세 기울기 임계값 (% 단위)
        self.trend_slope_threshold = 0.1  # 0.1% 기울기

        self.initialize_db()
        self.logger.info("ChartPatternAnalyzer 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chart_patterns (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    pattern_type VARCHAR(50) NOT NULL,
                    start_time_ms BIGINT NOT NULL,
                    end_time_ms BIGINT NOT NULL,
                    pattern_data JSONB NOT NULL,
                    confidence FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_chart_patterns_lookup 
                ON chart_patterns (symbol, timeframe, end_time_ms);

                CREATE TABLE IF NOT EXISTS support_resistance_levels (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    level_type VARCHAR(20) NOT NULL,
                    price_level FLOAT NOT NULL,
                    strength FLOAT NOT NULL,
                    touch_count INT NOT NULL,
                    start_time_ms BIGINT NOT NULL,
                    end_time_ms BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_support_resistance_lookup 
                ON support_resistance_levels (symbol, timeframe, end_time_ms);

                CREATE TABLE IF NOT EXISTS trend_lines (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    trend_type VARCHAR(20) NOT NULL,
                    start_time_ms BIGINT NOT NULL,
                    end_time_ms BIGINT NOT NULL,
                    start_price FLOAT NOT NULL,
                    end_price FLOAT NOT NULL,
                    touch_count INT NOT NULL,
                    slope FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_trend_lines_lookup 
                ON trend_lines (symbol, timeframe, end_time_ms);
            """)
            self.db_conn.commit()
            self.logger.info("차트 패턴 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def fetch_ohlcv_data(
            self,
            symbol: str,
            timeframe: str,
            limit: int = 200
    ) -> pd.DataFrame:
        """
        지정된 심볼과 시간프레임에 대한 OHLCV 데이터를 가져옵니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframe: 시간프레임(예: '1h')
            limit: 가져올 캔들 수

        Returns:
            pd.DataFrame: OHLCV 데이터프레임
        """
        self.logger.info(f"OHLCV 데이터 가져오는 중: {symbol} ({timeframe}), {limit}개 캔들")

        try:
            cursor = self.db_conn.cursor()

            # 심볼을 데이터베이스 형식에 맞게 변환
            db_symbol = symbol

            # 최신 캔들부터 가져와서 오래된 순으로 정렬
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

            # 타임스탬프를 datetime으로 변환 및 오름차순 정렬
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp')

            # 추가 피처 계산
            df['body_size'] = abs(df['close'] - df['open'])
            df['shadow_size'] = df['high'] - df['low']
            df['body_to_shadow_ratio'] = df['body_size'] / df['shadow_size']

            # 이동평균 계산
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['ma50'] = df['close'].rolling(window=50).mean()
            df['ma200'] = df['close'].rolling(window=200).mean()

            # 볼린저 밴드 계산
            window = 20
            df['std20'] = df['close'].rolling(window=window).std()
            df['upper_band'] = df['ma20'] + 2 * df['std20']
            df['lower_band'] = df['ma20'] - 2 * df['std20']

            # 인덱스 재설정
            df = df.reset_index(drop=True)

            self.logger.info(f"OHLCV 데이터 로드됨: {len(df)} 캔들")
            return df

        except Exception as e:
            self.logger.error(f"OHLCV 데이터 가져오기 실패: {e}")
            return pd.DataFrame()
        finally:
            if 'cursor' in locals():
                cursor.close()

    def identify_peaks_and_troughs(
            self,
            df: pd.DataFrame,
            window: int = 5,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        가격 데이터에서 피크(고점)와 트로프(저점)를 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            window: 피크 감지 윈도우 크기

        Returns:
            Tuple[np.ndarray, np.ndarray]: (피크 인덱스, 트로프 인덱스)
        """
        # 가격 배열
        prices = df['close'].values

        # 가격 변동성 계산
        price_range = df['high'].max() - df['low'].min()
        avg_price = df['close'].mean()
        volatility_pct = (price_range / avg_price) * 100

        # 충분한 변동성이 없으면 빈 배열 반환
        if volatility_pct < self.min_volatility_pct:
            self.logger.warning(f"변동성이 충분하지 않음: {volatility_pct:.2f}% < {self.min_volatility_pct}%")
            return np.array([]), np.array([])

        # 피크 감지 파라미터 조정
        prominence = avg_price * self.peak_prominence

        # 고점(피크) 식별
        peaks, _ = find_peaks(prices, distance=self.peak_distance, prominence=prominence)

        # 저점(트로프) 식별 (고점의 반대)
        troughs, _ = find_peaks(-prices, distance=self.peak_distance, prominence=prominence)

        self.logger.info(f"식별된 고점: {len(peaks)}개, 저점: {len(troughs)}개")
        return peaks, troughs

    def identify_support_resistance(
            self,
            df: pd.DataFrame,
            window_size: int = 20,
            price_threshold: float = 0.005
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        데이터에서 지지/저항 레벨을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            window_size: 분석 윈도우 크기
            price_threshold: 동일 가격 간주 임계값(%)

        Returns:
            Dict: 식별된 지지 및 저항 레벨
        """
        self.logger.info(f"지지/저항 레벨 식별 중: 윈도우 크기={window_size}, 임계값={price_threshold:.3f}")

        if df.empty or len(df) < window_size:
            self.logger.warning("데이터가 충분하지 않음")
            return {"support": [], "resistance": []}

        # 지지 레벨 후보: 저점
        support_candidates = []
        # 저항 레벨 후보: 고점
        resistance_candidates = []

        # 피크 및 트로프 식별
        peaks, troughs = self.identify_peaks_and_troughs(df, window=window_size)

        if len(peaks) == 0 or len(troughs) == 0:
            self.logger.warning("피크 또는 트로프가 식별되지 않음")
            return {"support": [], "resistance": []}

        # 평균 가격 계산
        avg_price = df['close'].mean()
        threshold = avg_price * price_threshold

        # 저점 그룹화 (지지 레벨)
        support_levels = []
        for trough in troughs:
            price = df.iloc[trough]['low']

            # 유사한 가격의 기존 레벨 찾기
            similar_level = None
            for level in support_levels:
                if abs(level['price'] - price) < threshold:
                    similar_level = level
                    break

            # 기존 레벨이 있으면 터치 카운트 증가
            if similar_level:
                similar_level['touch_count'] += 1
                similar_level['indices'].append(int(trough))
                # 가격 평균 업데이트
                similar_level['price'] = (similar_level['price'] * (similar_level['touch_count'] - 1) + price) / \
                                         similar_level['touch_count']
            else:
                # 새 레벨 추가
                support_levels.append({
                    'price': price,
                    'touch_count': 1,
                    'indices': [int(trough)],
                    'timestamp': df.iloc[trough]['timestamp']
                })

        # 고점 그룹화 (저항 레벨)
        resistance_levels = []
        for peak in peaks:
            price = df.iloc[peak]['high']

            # 유사한 가격의 기존 레벨 찾기
            similar_level = None
            for level in resistance_levels:
                if abs(level['price'] - price) < threshold:
                    similar_level = level
                    break

            # 기존 레벨이 있으면 터치 카운트 증가
            if similar_level:
                similar_level['touch_count'] += 1
                similar_level['indices'].append(int(peak))
                # 가격 평균 업데이트
                similar_level['price'] = (similar_level['price'] * (similar_level['touch_count'] - 1) + price) / \
                                         similar_level['touch_count']
            else:
                # 새 레벨 추가
                resistance_levels.append({
                    'price': price,
                    'touch_count': 1,
                    'indices': [int(peak)],
                    'timestamp': df.iloc[peak]['timestamp']
                })

        # 터치 횟수가 임계값 이상인 레벨만 필터링
        strong_support = [
            {
                'price': level['price'],
                'touch_count': level['touch_count'],
                'strength': level['touch_count'] / len(df) * 100,  # 강도를 백분율로 표현
                'start_time': df.iloc[min(level['indices'])]['timestamp'],
                'end_time': df.iloc[max(level['indices'])]['timestamp']
            }
            for level in support_levels
            if level['touch_count'] >= self.support_resistance_touches
        ]

        strong_resistance = [
            {
                'price': level['price'],
                'touch_count': level['touch_count'],
                'strength': level['touch_count'] / len(df) * 100,  # 강도를 백분율로 표현
                'start_time': df.iloc[min(level['indices'])]['timestamp'],
                'end_time': df.iloc[max(level['indices'])]['timestamp']
            }
            for level in resistance_levels
            if level['touch_count'] >= self.support_resistance_touches
        ]

        # 강도 기준 내림차순 정렬
        strong_support.sort(key=lambda x: x['strength'], reverse=True)
        strong_resistance.sort(key=lambda x: x['strength'], reverse=True)

        self.logger.info(f"식별된 지지 레벨: {len(strong_support)}개, 저항 레벨: {len(strong_resistance)}개")

        return {
            "support": strong_support,
            "resistance": strong_resistance
        }

    def identify_chart_patterns(
            self,
            df: pd.DataFrame,
            min_pattern_bars: int = 5
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 차트 패턴을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            min_pattern_bars: 패턴 식별을 위한 최소 바 수

        Returns:
            List[Dict]: 식별된 차트 패턴 목록
        """
        self.logger.info(f"차트 패턴 식별 중: 최소 패턴 바={min_pattern_bars}")

        if df.empty or len(df) < min_pattern_bars * 3:
            self.logger.warning("데이터가 충분하지 않음")
            return []

        patterns = []

        # 피크 및 트로프 식별
        peaks, troughs = self.identify_peaks_and_troughs(df)

        if len(peaks) < 2 or len(troughs) < 2:
            self.logger.warning("피크 또는 트로프가 충분하지 않음")
            return []

        # 모든 피크와 트로프를 인덱스로 정렬
        all_extrema = [(idx, 'peak') for idx in peaks] + [(idx, 'trough') for idx in troughs]
        all_extrema.sort(key=lambda x: x[0])

        # 두 바닥 (Double Bottom) 패턴 식별
        double_bottoms = self.identify_double_bottom(df, troughs)
        patterns.extend(double_bottoms)

        # 두 정상 (Double Top) 패턴 식별
        double_tops = self.identify_double_top(df, peaks)
        patterns.extend(double_tops)

        # 헤드앤숄더 (Head and Shoulders) 패턴 식별
        head_and_shoulders = self.identify_head_and_shoulders(df, peaks, troughs)
        patterns.extend(head_and_shoulders)

        # 역 헤드앤숄더 (Inverse Head and Shoulders) 패턴 식별
        inv_head_and_shoulders = self.identify_inverse_head_and_shoulders(df, peaks, troughs)
        patterns.extend(inv_head_and_shoulders)

        # 삼각형 (Triangle) 패턴 식별
        triangles = self.identify_triangle_patterns(df, peaks, troughs)
        patterns.extend(triangles)

        # 플래그 (Flag) 패턴 식별
        flags = self.identify_flag_patterns(df, peaks, troughs)
        patterns.extend(flags)

        self.logger.info(f"식별된 차트 패턴: {len(patterns)}개")
        return patterns

    def identify_double_bottom(
            self,
            df: pd.DataFrame,
            troughs: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 두 바닥(Double Bottom) 패턴을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            troughs: 저점 인덱스 배열

        Returns:
            List[Dict]: 식별된 두 바닥 패턴 목록
        """
        patterns = []

        if len(troughs) < 2:
            return patterns

        # 연속된 두 저점 쌍 검사
        for i in range(len(troughs) - 1):
            trough1_idx = troughs[i]
            trough2_idx = troughs[i + 1]

            # 두 저점 사이 간격이 충분한지 확인
            if trough2_idx - trough1_idx < self.min_pattern_bars:
                continue

            trough1_price = df.iloc[trough1_idx]['low']
            trough2_price = df.iloc[trough2_idx]['low']

            # 두 저점의 가격이 유사한지 확인 (5% 이내)
            price_diff_pct = abs(trough2_price - trough1_price) / trough1_price * 100
            if price_diff_pct > 5:
                continue

            # 두 저점 사이에 중간 고점이 있는지 확인
            mid_section = df.iloc[trough1_idx:trough2_idx]
            mid_high = mid_section['high'].max()
            mid_high_idx = mid_section['high'].idxmax()

            # 중간 고점이 충분히 높은지 확인 (저점보다 5% 이상 높아야 함)
            height_diff_pct = (mid_high - min(trough1_price, trough2_price)) / min(trough1_price, trough2_price) * 100
            if height_diff_pct < 5:
                continue

            # 패턴 이후에 확증이 있는지 확인 (두 번째 저점 이후 중간 고점을 돌파)
            if trough2_idx + 5 >= len(df):
                continue

            confirmation_section = df.iloc[trough2_idx:min(trough2_idx + 10, len(df))]
            if not (confirmation_section['high'] > mid_high).any():
                continue

            # 확증 지점 인덱스
            confirmation_idx = confirmation_section[confirmation_section['high'] > mid_high].index[0]

            # 신뢰도 계산 (패턴 특성에 따라 0-100%)
            confidence = min(100, 50 + height_diff_pct - price_diff_pct)

            # 패턴 추가
            pattern = {
                'pattern_type': 'double_bottom',
                'start_idx': int(trough1_idx),
                'end_idx': int(confirmation_idx),
                'trough1_idx': int(trough1_idx),
                'trough2_idx': int(trough2_idx),
                'middle_peak_idx': int(mid_high_idx),
                'confirmation_idx': int(confirmation_idx),
                'trough1_price': float(trough1_price),
                'trough2_price': float(trough2_price),
                'middle_peak_price': float(mid_high),
                'confidence': float(confidence),
                'target_price': float(mid_high + (mid_high - min(trough1_price, trough2_price))),
                'stop_loss_price': float(min(trough1_price, trough2_price) * 0.99)
            }

            patterns.append(pattern)

        return patterns

    def identify_double_top(
            self,
            df: pd.DataFrame,
            peaks: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 두 정상(Double Top) 패턴을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            peaks: 고점 인덱스 배열

        Returns:
            List[Dict]: 식별된 두 정상 패턴 목록
        """
        patterns = []

        if len(peaks) < 2:
            return patterns

        # 연속된 두 고점 쌍 검사
        for i in range(len(peaks) - 1):
            peak1_idx = peaks[i]
            peak2_idx = peaks[i + 1]

            # 두 고점 사이 간격이 충분한지 확인
            if peak2_idx - peak1_idx < self.min_pattern_bars:
                continue

            peak1_price = df.iloc[peak1_idx]['high']
            peak2_price = df.iloc[peak2_idx]['high']

            # 두 고점의 가격이 유사한지 확인 (5% 이내)
            price_diff_pct = abs(peak2_price - peak1_price) / peak1_price * 100
            if price_diff_pct > 5:
                continue

            # 두 고점 사이에 중간 저점이 있는지 확인
            mid_section = df.iloc[peak1_idx:peak2_idx]
            mid_low = mid_section['low'].min()
            mid_low_idx = mid_section['low'].idxmin()

            # 중간 저점이 충분히 낮은지 확인 (고점보다 5% 이상 낮아야 함)
            depth_diff_pct = (min(peak1_price, peak2_price) - mid_low) / mid_low * 100
            if depth_diff_pct < 5:
                continue

            # 패턴 이후에 확증이 있는지 확인 (두 번째 고점 이후 중간 저점을 하향 돌파)
            if peak2_idx + 5 >= len(df):
                continue

            confirmation_section = df.iloc[peak2_idx:min(peak2_idx + 10, len(df))]
            if not (confirmation_section['low'] < mid_low).any():
                continue

            # 확증 지점 인덱스
            confirmation_idx = confirmation_section[confirmation_section['low'] < mid_low].index[0]

            # 신뢰도 계산 (패턴 특성에 따라 0-100%)
            confidence = min(100, 50 + depth_diff_pct - price_diff_pct)

            # 패턴 추가
            pattern = {
                'pattern_type': 'double_top',
                'start_idx': int(peak1_idx),
                'end_idx': int(confirmation_idx),
                'peak1_idx': int(peak1_idx),
                'peak2_idx': int(peak2_idx),
                'middle_trough_idx': int(mid_low_idx),
                'confirmation_idx': int(confirmation_idx),
                'peak1_price': float(peak1_price),
                'peak2_price': float(peak2_price),
                'middle_trough_price': float(mid_low),
                'confidence': float(confidence),
                'target_price': float(mid_low - (max(peak1_price, peak2_price) - mid_low)),
                'stop_loss_price': float(max(peak1_price, peak2_price) * 1.01)
            }

            patterns.append(pattern)

        return patterns

    def identify_head_and_shoulders(
            self,
            df: pd.DataFrame,
            peaks: np.ndarray,
            troughs: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 헤드앤숄더(Head and Shoulders) 패턴을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            peaks: 고점 인덱스 배열
            troughs: 저점 인덱스 배열

        Returns:
            List[Dict]: 식별된 헤드앤숄더 패턴 목록
        """
        patterns = []

        if len(peaks) < 3 or len(troughs) < 2:
            return patterns

        # 연속된 세 고점 검사
        for i in range(len(peaks) - 2):
            left_shoulder_idx = peaks[i]
            head_idx = peaks[i + 1]
            right_shoulder_idx = peaks[i + 2]

            # 각 부분 간격이 충분한지 확인
            if head_idx - left_shoulder_idx < self.min_pattern_bars or right_shoulder_idx - head_idx < self.min_pattern_bars:
                continue

            left_shoulder_price = df.iloc[left_shoulder_idx]['high']
            head_price = df.iloc[head_idx]['high']
            right_shoulder_price = df.iloc[right_shoulder_idx]['high']

            # 헤드가 양쪽 숄더보다 높은지 확인
            if head_price <= left_shoulder_price or head_price <= right_shoulder_price:
                continue

            # 양쪽 숄더의 가격이 유사한지 확인 (15% 이내)
            shoulder_diff_pct = abs(right_shoulder_price - left_shoulder_price) / left_shoulder_price * 100
            if shoulder_diff_pct > 15:
                continue

            # 넥라인(Neckline) 식별을 위한 저점 찾기
            left_trough_candidates = [t for t in troughs if left_shoulder_idx < t < head_idx]
            right_trough_candidates = [t for t in troughs if head_idx < t < right_shoulder_idx]

            if not left_trough_candidates or not right_trough_candidates:
                continue

            left_trough_idx = left_trough_candidates[-1]  # 헤드에 가장 가까운 저점
            right_trough_idx = right_trough_candidates[0]  # 헤드에 가장 가까운 저점

            left_trough_price = df.iloc[left_trough_idx]['low']
            right_trough_price = df.iloc[right_trough_idx]['low']

            # 넥라인 기울기 계산
            neckline_slope = (right_trough_price - left_trough_price) / (right_trough_idx - left_trough_idx)

            # 패턴 이후에 확증이 있는지 확인 (넥라인 하향 돌파)
            if right_shoulder_idx + 3 >= len(df):
                continue

            # 넥라인 확장하여 돌파 지점에서의 가격 계산
            confirmation_section = df.iloc[right_shoulder_idx:min(right_shoulder_idx + 15, len(df))]

            # 각 인덱스에서의 넥라인 가격 계산
            has_confirmation = False
            confirmation_idx = right_shoulder_idx

            for idx in confirmation_section.index:
                bars_from_left_trough = idx - left_trough_idx
                neckline_price_at_idx = left_trough_price + neckline_slope * bars_from_left_trough

                if df.loc[idx, 'close'] < neckline_price_at_idx:
                    has_confirmation = True
                    confirmation_idx = idx
                    break

            if not has_confirmation:
                continue

            # 신뢰도 계산
            head_height = head_price - min(left_trough_price, right_trough_price)
            height_to_pattern_ratio = head_height / head_price * 100

            confidence = min(100, 40 + height_to_pattern_ratio - shoulder_diff_pct)

            # 목표가 계산 (넥라인에서 헤드 높이만큼 하락)
            target_price = right_trough_price - head_height

            # 패턴 추가
            pattern = {
                'pattern_type': 'head_and_shoulders',
                'start_idx': int(left_shoulder_idx),
                'end_idx': int(confirmation_idx),
                'left_shoulder_idx': int(left_shoulder_idx),
                'head_idx': int(head_idx),
                'right_shoulder_idx': int(right_shoulder_idx),
                'left_trough_idx': int(left_trough_idx),
                'right_trough_idx': int(right_trough_idx),
                'confirmation_idx': int(confirmation_idx),
                'left_shoulder_price': float(left_shoulder_price),
                'head_price': float(head_price),
                'right_shoulder_price': float(right_shoulder_price),
                'left_trough_price': float(left_trough_price),
                'right_trough_price': float(right_trough_price),
                'neckline_slope': float(neckline_slope),
                'confidence': float(confidence),
                'target_price': float(target_price),
                'stop_loss_price': float(head_price * 1.01)
            }

            patterns.append(pattern)

        return patterns

    def identify_inverse_head_and_shoulders(
            self,
            df: pd.DataFrame,
            peaks: np.ndarray,
            troughs: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 역 헤드앤숄더(Inverse Head and Shoulders) 패턴을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            peaks: 고점 인덱스 배열
            troughs: 저점 인덱스 배열

        Returns:
            List[Dict]: 식별된 역 헤드앤숄더 패턴 목록
        """
        patterns = []

        if len(troughs) < 3 or len(peaks) < 2:
            return patterns

        # 연속된 세 저점 검사
        for i in range(len(troughs) - 2):
            left_shoulder_idx = troughs[i]
            head_idx = troughs[i + 1]
            right_shoulder_idx = troughs[i + 2]

            # 각 부분 간격이 충분한지 확인
            if head_idx - left_shoulder_idx < self.min_pattern_bars or right_shoulder_idx - head_idx < self.min_pattern_bars:
                continue

            left_shoulder_price = df.iloc[left_shoulder_idx]['low']
            head_price = df.iloc[head_idx]['low']
            right_shoulder_price = df.iloc[right_shoulder_idx]['low']

            # 헤드가 양쪽 숄더보다 낮은지 확인
            if head_price >= left_shoulder_price or head_price >= right_shoulder_price:
                continue

            # 양쪽 숄더의 가격이 유사한지 확인 (15% 이내)
            shoulder_diff_pct = abs(right_shoulder_price - left_shoulder_price) / left_shoulder_price * 100
            if shoulder_diff_pct > 15:
                continue

            # 넥라인(Neckline) 식별을 위한 고점 찾기
            left_peak_candidates = [p for p in peaks if left_shoulder_idx < p < head_idx]
            right_peak_candidates = [p for p in peaks if head_idx < p < right_shoulder_idx]

            if not left_peak_candidates or not right_peak_candidates:
                continue

            left_peak_idx = left_peak_candidates[-1]  # 헤드에 가장 가까운 고점
            right_peak_idx = right_peak_candidates[0]  # 헤드에 가장 가까운 고점

            left_peak_price = df.iloc[left_peak_idx]['high']
            right_peak_price = df.iloc[right_peak_idx]['high']

            # 넥라인 기울기 계산
            neckline_slope = (right_peak_price - left_peak_price) / (right_peak_idx - left_peak_idx)

            # 패턴 이후에 확증이 있는지 확인 (넥라인 상향 돌파)
            if right_shoulder_idx + 3 >= len(df):
                continue

            # 넥라인 확장하여 돌파 지점에서의 가격 계산
            confirmation_section = df.iloc[right_shoulder_idx:min(right_shoulder_idx + 15, len(df))]

            # 각 인덱스에서의 넥라인 가격 계산
            has_confirmation = False
            confirmation_idx = right_shoulder_idx

            for idx in confirmation_section.index:
                bars_from_left_peak = idx - left_peak_idx
                neckline_price_at_idx = left_peak_price + neckline_slope * bars_from_left_peak

                if df.loc[idx, 'close'] > neckline_price_at_idx:
                    has_confirmation = True
                    confirmation_idx = idx
                    break

            if not has_confirmation:
                continue

            # 신뢰도 계산
            head_depth = max(left_peak_price, right_peak_price) - head_price
            depth_to_pattern_ratio = head_depth / head_price * 100

            confidence = min(100, 40 + depth_to_pattern_ratio - shoulder_diff_pct)

            # 목표가 계산 (넥라인에서 헤드 깊이만큼 상승)
            target_price = right_peak_price + head_depth

            # 패턴 추가
            pattern = {
                'pattern_type': 'inverse_head_and_shoulders',
                'start_idx': int(left_shoulder_idx),
                'end_idx': int(confirmation_idx),
                'left_shoulder_idx': int(left_shoulder_idx),
                'head_idx': int(head_idx),
                'right_shoulder_idx': int(right_shoulder_idx),
                'left_peak_idx': int(left_peak_idx),
                'right_peak_idx': int(right_peak_idx),
                'confirmation_idx': int(confirmation_idx),
                'left_shoulder_price': float(left_shoulder_price),
                'head_price': float(head_price),
                'right_shoulder_price': float(right_shoulder_price),
                'left_peak_price': float(left_peak_price),
                'right_peak_price': float(right_peak_price),
                'neckline_slope': float(neckline_slope),
                'confidence': float(confidence),
                'target_price': float(target_price),
                'stop_loss_price': float(head_price * 0.99)
            }

            patterns.append(pattern)

        return patterns

    def identify_triangle_patterns(
            self,
            df: pd.DataFrame,
            peaks: np.ndarray,
            troughs: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 삼각형 패턴(대칭, 상승, 하락)을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            peaks: 고점 인덱스 배열
            troughs: 저점 인덱스 배열

        Returns:
            List[Dict]: 식별된 삼각형 패턴 목록
        """
        patterns = []

        if len(peaks) < 3 or len(troughs) < 3:
            return patterns

        # 적어도 최근 패턴만 분석 (계산 복잡성 감소)
        max_lookback = 100
        recent_df = df.iloc[-max_lookback:] if len(df) > max_lookback else df

        # 최근 데이터에서 피크와 트로프 재구성
        recent_peaks = [p for p in peaks if p >= recent_df.index[0]]
        recent_troughs = [t for t in troughs if t >= recent_df.index[0]]

        if len(recent_peaks) < 3 or len(recent_troughs) < 3:
            return patterns

        # 상단 추세선 구성 (고점 연결)
        upper_trendline = self.fit_trendline(df, recent_peaks, 'high')

        # 하단 추세선 구성 (저점 연결)
        lower_trendline = self.fit_trendline(df, recent_troughs, 'low')

        if not upper_trendline or not lower_trendline:
            return patterns

        upper_slope = upper_trendline['slope']
        lower_slope = lower_trendline['slope']

        # 시작 및 종료 인덱스 결정
        start_idx = min(upper_trendline['start_idx'], lower_trendline['start_idx'])
        latest_idx = recent_df.index[-1]

        # 최근 5개 캔들에서 삼각형 확인
        triangle_confirmed = False
        confirmation_idx = latest_idx

        # 삼각형 패턴 유형 결정
        if abs(upper_slope) < 0.001 and abs(lower_slope) < 0.001:  # 둘 다 수평
            return patterns  # 삼각형 아님

        elif abs(upper_slope) < 0.001 and lower_slope > 0:  # 상단 수평, 하단 상승
            pattern_type = 'ascending_triangle'
            triangle_confirmed = True

        elif upper_slope < 0 and abs(lower_slope) < 0.001:  # 상단 하락, 하단 수평
            pattern_type = 'descending_triangle'
            triangle_confirmed = True

        elif upper_slope < 0 and lower_slope > 0:  # 상단 하락, 하단 상승
            pattern_type = 'symmetrical_triangle'
            triangle_confirmed = True

        else:
            return patterns  # 삼각형 조건 불충족

        if triangle_confirmed:
            # 확인 시점에서의 삼각형 너비 계산
            triangle_width_start = upper_trendline['start_price'] - lower_trendline['start_price']

            # 현재 시점에서의 추세선 가격 계산
            bars_from_upper_start = latest_idx - upper_trendline['start_idx']
            upper_price_now = upper_trendline['start_price'] + upper_slope * bars_from_upper_start

            bars_from_lower_start = latest_idx - lower_trendline['start_idx']
            lower_price_now = lower_trendline['start_price'] + lower_slope * bars_from_lower_start

            triangle_width_now = upper_price_now - lower_price_now

            # 삼각형 너비가 충분히 좁아졌는지 확인 (시작의 50% 이하)
            if triangle_width_now > triangle_width_start * 0.5:
                return patterns  # 아직 충분히 좁아지지 않음

            # 삼각형 높이 계산
            triangle_height = triangle_width_start

            # 목표가 계산 (브레이크아웃 방향으로 삼각형 높이만큼)
            if pattern_type == 'ascending_triangle':
                target_price = upper_price_now + triangle_height
                stop_loss_price = lower_price_now * 0.99
            elif pattern_type == 'descending_triangle':
                target_price = lower_price_now - triangle_height
                stop_loss_price = upper_price_now * 1.01
            else:  # symmetrical_triangle - 추세 방향에 따라 결정
                trend = 'bullish' if df.iloc[-20:]['close'].mean() > df.iloc[-40:-20]['close'].mean() else 'bearish'

                if trend == 'bullish':
                    target_price = upper_price_now + triangle_height
                    stop_loss_price = lower_price_now * 0.99
                else:
                    target_price = lower_price_now - triangle_height
                    stop_loss_price = upper_price_now * 1.01

            # 신뢰도 계산
            touch_count = min(upper_trendline['touch_count'], lower_trendline['touch_count'])
            width_reduction_pct = (triangle_width_start - triangle_width_now) / triangle_width_start * 100

            confidence = min(100, 40 + touch_count * 10 + width_reduction_pct / 2)

            # 패턴 추가
            pattern = {
                'pattern_type': pattern_type,
                'start_idx': int(start_idx),
                'end_idx': int(latest_idx),
                'upper_trendline': upper_trendline,
                'lower_trendline': lower_trendline,
                'pattern_width_start': float(triangle_width_start),
                'pattern_width_now': float(triangle_width_now),
                'width_reduction_pct': float(width_reduction_pct),
                'confidence': float(confidence),
                'target_price': float(target_price),
                'stop_loss_price': float(stop_loss_price)
            }

            patterns.append(pattern)

        return patterns

    def identify_flag_patterns(
            self,
            df: pd.DataFrame,
            peaks: np.ndarray,
            troughs: np.ndarray
    ) -> List[Dict[str, Any]]:
        """
        데이터에서 플래그 패턴(깃발 패턴)을 식별합니다.

        Args:
            df: OHLCV 데이터프레임
            peaks: 고점 인덱스 배열
            troughs: 저점 인덱스 배열

        Returns:
            List[Dict]: 식별된 플래그 패턴 목록
        """
        patterns = []

        if len(df) < 20:
            return patterns

        # 적어도 최근 패턴만 분석 (계산 복잡성 감소)
        max_lookback = 100
        recent_df = df.iloc[-max_lookback:] if len(df) > max_lookback else df

        # 최근 10-30 캔들의 추세 확인
        recent_trend_section = recent_df.iloc[-30:-5] if len(recent_df) > 30 else recent_df.iloc[:-5]

        if len(recent_trend_section) < 10:
            return patterns

        # 폴(pole) 식별 - 강한 단방향 움직임
        price_change = recent_trend_section['close'].iloc[-1] - recent_trend_section['close'].iloc[0]
        price_change_pct = price_change / recent_trend_section['close'].iloc[0] * 100

        is_strong_trend = abs(price_change_pct) > 10  # 10% 이상 변화를 강한 추세로 간주
        trend_direction = 'bullish' if price_change > 0 else 'bearish'

        if not is_strong_trend:
            return patterns

        # 플래그 섹션 (최근 5-15 캔들)
        flag_section = recent_df.iloc[-15:-1] if len(recent_df) > 15 else recent_df.iloc[-len(recent_df) // 3:]

        if len(flag_section) < 5:
            return patterns

        # 플래그 피크 및 트로프 식별
        flag_peaks = [p for p in peaks if p in flag_section.index]
        flag_troughs = [t for t in troughs if t in flag_section.index]

        if len(flag_peaks) < 2 or len(flag_troughs) < 2:
            return patterns

        # 플래그 채널의 상단 및 하단 추세선 식별
        upper_trendline = self.fit_trendline(df, flag_peaks, 'high')
        lower_trendline = self.fit_trendline(df, flag_troughs, 'low')

        if not upper_trendline or not lower_trendline:
            return patterns

        # 플래그 채널의 기울기 확인 (불리시 플래그는 하락 채널, 베어리시 플래그는 상승 채널)
        upper_slope = upper_trendline['slope']
        lower_slope = lower_trendline['slope']

        # 추세와 플래그 기울기가 반대 방향인지 확인
        if (trend_direction == 'bullish' and (upper_slope > 0 or lower_slope > 0)) or \
                (trend_direction == 'bearish' and (upper_slope < 0 or lower_slope < 0)):
            return patterns

        # 플래그 채널의 너비가 일정한지 확인
        channel_width_start = upper_trendline['start_price'] - lower_trendline['start_price']

        latest_idx = flag_section.index[-1]

        bars_from_upper_start = latest_idx - upper_trendline['start_idx']
        upper_price_now = upper_trendline['start_price'] + upper_slope * bars_from_upper_start

        bars_from_lower_start = latest_idx - lower_trendline['start_idx']
        lower_price_now = lower_trendline['start_price'] + lower_slope * bars_from_lower_start

        channel_width_now = upper_price_now - lower_price_now

        # 채널 너비 변화가 작은지 확인
        width_change_pct = abs(channel_width_now - channel_width_start) / channel_width_start * 100

        if width_change_pct > 30:  # 30% 이상 변화는 플래그가 아님
            return patterns

        # 폴 높이 계산 (향후 목표가 설정에 사용)
        pole_section = recent_trend_section
        pole_height = abs(pole_section['close'].max() - pole_section['close'].min())

        # 확인 신호 - 플래그 채널의 돌파
        has_confirmation = False
        confirmation_idx = latest_idx
        confirmation_price = 0

        # 마지막 캔들의 종가가 채널을 돌파했는지 확인
        if ((trend_direction == 'bullish' and df.iloc[-1]['close'] > upper_price_now) or
                (trend_direction == 'bearish' and df.iloc[-1]['close'] < lower_price_now)):
            has_confirmation = True
            confirmation_idx = df.index[-1]
            confirmation_price = df.iloc[-1]['close']

        if not has_confirmation:
            # 아직 돌파하지 않았지만 패턴은 유효함
            pattern_type = 'bullish_flag' if trend_direction == 'bullish' else 'bearish_flag'

            # 패턴 발견 보고 (아직 확인되지 않음)
            pattern = {
                'pattern_type': pattern_type,
                'start_idx': int(recent_trend_section.index[0]),
                'end_idx': int(latest_idx),
                'pole_start_idx': int(recent_trend_section.index[0]),
                'pole_end_idx': int(recent_trend_section.index[-1]),
                'flag_start_idx': int(flag_section.index[0]),
                'flag_end_idx': int(flag_section.index[-1]),
                'upper_trendline': upper_trendline,
                'lower_trendline': lower_trendline,
                'confirmation': False,
                'confidence': float(50),  # 확인되지 않은 패턴은 50% 신뢰도
                'target_price': float(confirmation_price + pole_height if trend_direction == 'bullish' else
                                      confirmation_price - pole_height),
                'stop_loss_price': float(lower_price_now * 0.99 if trend_direction == 'bullish' else
                                         upper_price_now * 1.01)
            }

            patterns.append(pattern)

        return patterns

    def fit_trendline(
            self,
            df: pd.DataFrame,
            points: List[int],
            price_type: str = 'close'
    ) -> Optional[Dict[str, Any]]:
        """
        주어진 점들에 맞는 추세선을 계산합니다.

        Args:
            df: OHLCV 데이터프레임
            points: 추세선을 그릴 포인트 인덱스 리스트
            price_type: 사용할 가격 유형('high', 'low', 'close')

        Returns:
            Optional[Dict]: 추세선 정보 딕셔너리 또는 None
        """
        if len(points) < 2:
            return None

        # x, y 값 준비
        x = np.array(points)
        y = np.array([df.iloc[p][price_type] for p in points])

        # 선형 회귀 계산
        slope, intercept = np.polyfit(x, y, 1)

        # 터치 포인트 계산 (추세선과 가까운 모든 포인트)
        touch_count = 0
        touch_indices = []

        for i in range(min(points), max(points) + 1):
            if i not in df.index:
                continue

            line_price = slope * i + intercept
            price = df.iloc[i][price_type]

            # 가격이 추세선과 0.5% 이내인지 확인
            price_diff_pct = abs(price - line_price) / line_price * 100

            if price_diff_pct < 0.5:
                touch_count += 1
                touch_indices.append(i)

        return {
            'start_idx': int(min(points)),
            'end_idx': int(max(points)),
            'start_price': float(slope * min(points) + intercept),
            'end_price': float(slope * max(points) + intercept),
            'slope': float(slope),
            'intercept': float(intercept),
            'touch_count': touch_count,
            'touch_indices': touch_indices
        }

    def calculate_fibonacci_levels(
            self,
            high: float,
            low: float,
            trend: str = 'uptrend'
    ) -> Dict[str, float]:
        """
        피보나치 레벨을 계산합니다.

        Args:
            high: 높은 가격
            low: 낮은 가격
            trend: 추세 방향 ('uptrend' 또는 'downtrend')

        Returns:
            Dict[str, float]: 피보나치 레벨 딕셔너리
        """
        diff = high - low

        levels = {
            '0.0': low if trend == 'uptrend' else high,
            '0.236': low + 0.236 * diff if trend == 'uptrend' else high - 0.236 * diff,
            '0.382': low + 0.382 * diff if trend == 'uptrend' else high - 0.382 * diff,
            '0.5': low + 0.5 * diff if trend == 'uptrend' else high - 0.5 * diff,
            '0.618': low + 0.618 * diff if trend == 'uptrend' else high - 0.618 * diff,
            '0.786': low + 0.786 * diff if trend == 'uptrend' else high - 0.786 * diff,
            '1.0': high if trend == 'uptrend' else low
        }

        return levels

    def analyze_chart(
            self,
            symbol: str,
            timeframe: str,
            limit: int = 200
    ) -> Dict[str, Any]:
        """
        차트를 분석하여 패턴, 지지/저항 레벨, 추세선을 식별합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframe: 시간프레임(예: '1h')
            limit: 분석할 캔들 수

        Returns:
            Dict: 분석 결과
        """
        self.logger.info(f"차트 분석 시작: {symbol} ({timeframe})")

        result = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp": int(time.time() * 1000),
            "patterns": [],
            "support_resistance": {
                "support": [],
                "resistance": []
            },
            "fibonacci_levels": {},
            "trend": "neutral"
        }

        try:
            # OHLCV 데이터 가져오기
            df = self.fetch_ohlcv_data(symbol, timeframe, limit)

            if df.empty:
                self.logger.warning(f"분석할 데이터가 없음: {symbol} ({timeframe})")
                return result

            # 차트 패턴 식별
            patterns = self.identify_chart_patterns(df)
            result["patterns"] = patterns

            # 지지/저항 레벨 식별
            support_resistance = self.identify_support_resistance(df)
            result["support_resistance"] = support_resistance

            # 고점과 저점 찾기
            high = df['high'].max()
            low = df['low'].min()

            # 현재 추세 판단
            ma20 = df['ma20'].iloc[-1] if not pd.isna(df['ma20'].iloc[-1]) else \
                df['close'].rolling(window=20).mean().iloc[-1]
            ma50 = df['ma50'].iloc[-1] if not pd.isna(df['ma50'].iloc[-1]) else \
                df['close'].rolling(window=50).mean().iloc[-1]

            current_price = df['close'].iloc[-1]

            if current_price > ma50 and ma20 > ma50:
                trend = "uptrend"
            elif current_price < ma50 and ma20 < ma50:
                trend = "downtrend"
            else:
                trend = "neutral"

            result["trend"] = trend

            # 피보나치 레벨 계산
            fib_levels = self.calculate_fibonacci_levels(high, low, trend)
            result["fibonacci_levels"] = fib_levels

            # 최근 패턴에서 TP/SL 추출
            if patterns:
                # 신뢰도 순으로 정렬
                sorted_patterns = sorted(patterns, key=lambda x: x.get('confidence', 0), reverse=True)

                result["recommended_tp"] = sorted_patterns[0].get('target_price')
                result["recommended_sl"] = sorted_patterns[0].get('stop_loss_price')
                result["recommended_pattern"] = sorted_patterns[0].get('pattern_type')

            self.logger.info(f"차트 분석 완료: {symbol} ({timeframe}), 패턴 {len(patterns)}개 발견")

        except Exception as e:
            self.logger.error(f"차트 분석 중 오류 발생: {e}")

        return result

    def save_analysis_results(
            self,
            symbol: str,
            timeframe: str,
            results: Dict[str, Any]
    ) -> None:
        """
        분석 결과를 Redis 및 PostgreSQL에 저장합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframe: 시간프레임(예: '1h')
            results: 분석 결과
        """
        # Redis에 저장
        redis_key = f"analysis:chart:{symbol.replace('/', '_')}:{timeframe}"
        self.redis_client.set(redis_key, json.dumps(results))
        self.redis_client.expire(redis_key, 86400)  # 1일 후 만료

        # 패턴 저장
        cursor = self.db_conn.cursor()

        try:
            # 기존 패턴 삭제 (최신 결과만 유지)
            cursor.execute(
                """
                DELETE FROM chart_patterns
                WHERE symbol = %s AND timeframe = %s AND
                created_at < (NOW() - INTERVAL '1 day')
                """,
                (symbol, timeframe)
            )

            # 새 패턴 추가
            for pattern in results.get('patterns', []):
                cursor.execute(
                    """
                    INSERT INTO chart_patterns
                    (symbol, timeframe, pattern_type, start_time_ms, end_time_ms, pattern_data, confidence)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        symbol,
                        timeframe,
                        pattern.get('pattern_type', 'unknown'),
                        int(time.time() * 1000) - (pattern.get('end_idx', 0) - pattern.get('start_idx', 0)) * 60000,
                        int(time.time() * 1000),
                        json.dumps(pattern),
                        pattern.get('confidence', 50)
                    )
                )

            # 지지/저항 레벨 저장
            for support in results.get('support_resistance', {}).get('support', []):
                cursor.execute(
                    """
                    INSERT INTO support_resistance_levels
                    (symbol, timeframe, level_type, price_level, strength, touch_count, start_time_ms, end_time_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        symbol,
                        timeframe,
                        'support',
                        support.get('price', 0),
                        support.get('strength', 0),
                        support.get('touch_count', 0),
                        int(time.time() * 1000) - 86400000,  # 1일 전
                        int(time.time() * 1000)
                    )
                )

            for resistance in results.get('support_resistance', {}).get('resistance', []):
                cursor.execute(
                    """
                    INSERT INTO support_resistance_levels
                    (symbol, timeframe, level_type, price_level, strength, touch_count, start_time_ms, end_time_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        symbol,
                        timeframe,
                        'resistance',
                        resistance.get('price', 0),
                        resistance.get('strength', 0),
                        resistance.get('touch_count', 0),
                        int(time.time() * 1000) - 86400000,  # 1일 전
                        int(time.time() * 1000)
                    )
                )

            self.db_conn.commit()
            self.logger.info(f"분석 결과 저장됨: {symbol} ({timeframe})")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"분석 결과 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

    def run_analysis(
            self,
            symbol: str = "BTC/USDT",
            timeframes: List[str] = ["1h", "4h", "1d"]
    ) -> Dict[str, Dict[str, Any]]:
        """
        여러 시간프레임에 대한 차트 분석을 실행합니다.

        Args:
            symbol: 심볼(예: 'BTC/USDT')
            timeframes: 분석할 시간프레임 목록

        Returns:
            Dict: 시간프레임별 분석 결과
        """
        self.logger.info(f"차트 분석 시작: {symbol}, 시간프레임: {timeframes}")

        results = {}

        for timeframe in timeframes:
            try:
                analysis_result = self.analyze_chart(symbol, timeframe)
                self.save_analysis_results(symbol, timeframe, analysis_result)
                results[timeframe] = analysis_result

            except Exception as e:
                self.logger.error(f"{timeframe} 분석 중 오류 발생: {e}")
                results[timeframe] = {"error": str(e)}

        return results

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")

        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            self.logger.info("Redis 연결 종료됨")


# 직접 실행 시 분석기 시작
if __name__ == "__main__":
    analyzer = ChartPatternAnalyzer()

    try:
        results = analyzer.run_analysis("BTC/USDT", ["1h", "4h", "1d"])

        for timeframe, result in results.items():
            print(f"[{timeframe}] 패턴 수: {len(result.get('patterns', []))}, 추세: {result.get('trend')}")

            if result.get('patterns'):
                top_pattern = sorted(result['patterns'], key=lambda x: x.get('confidence', 0), reverse=True)[0]
                print(f"  최고 신뢰도 패턴: {top_pattern.get('pattern_type')} ({top_pattern.get('confidence', 0):.1f}%)")
                print(
                    f"  목표가: {top_pattern.get('target_price', 0):.2f}, 손절가: {top_pattern.get('stop_loss_price', 0):.2f}")

    except Exception as e:
        print(f"분석 오류: {e}")
    finally:
        analyzer.close()
