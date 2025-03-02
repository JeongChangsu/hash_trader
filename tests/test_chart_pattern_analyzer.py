# tests/test_chart_pattern_analyzer.py
"""
차트 패턴 분석기 테스트 모듈

이 모듈은 chart_pattern_analyzer.py의 기능을 테스트합니다.
"""

import sys
import os
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 상위 디렉토리를 sys.path에 추가하여 모듈 임포트 가능하게 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzers.chart_pattern_analyzer import ChartPatternAnalyzer


class TestChartPatternAnalyzer(unittest.TestCase):
    """차트 패턴 분석기 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        # 테스트를 위한 가상 OHLCV 데이터 생성
        self.sample_data = self.create_sample_data()

        # 테스트 대상 인스턴스 생성
        self.analyzer = ChartPatternAnalyzer()

        # DB 연결 대신 가상 메서드 사용
        self.analyzer.fetch_ohlcv_data = lambda symbol, timeframe, limit: self.sample_data

    def create_sample_data(self):
        """테스트용 가상 OHLCV 데이터 생성"""
        # 날짜 범위 생성
        dates = [datetime.now() - timedelta(days=i) for i in range(100, 0, -1)]

        # 기본 데이터 생성
        data = {
            'timestamp': dates,
            'open': np.random.normal(10000, 200, 100),
            'high': np.zeros(100),
            'low': np.zeros(100),
            'close': np.zeros(100),
            'volume': np.random.normal(100, 30, 100)
        }

        # 상승 추세 패턴 생성
        for i in range(100):
            data['close'][i] = data['open'][i] * (1 + 0.001 * i) + np.random.normal(0, 100)
            data['high'][i] = max(data['open'][i], data['close'][i]) + abs(np.random.normal(0, 50))
            data['low'][i] = min(data['open'][i], data['close'][i]) - abs(np.random.normal(0, 50))

        # 이중 바닥 패턴 생성 (60-80 구간)
        data['close'][60] = data['close'][59] * 0.95
        data['close'][61] = data['close'][60] * 0.97
        data['close'][62] = data['close'][61] * 0.99
        data['close'][63] = data['close'][62] * 1.02
        data['close'][64] = data['close'][63] * 1.03
        data['close'][65] = data['close'][64] * 1.01

        data['close'][75] = data['close'][74] * 0.96
        data['close'][76] = data['close'][75] * 0.98
        data['close'][77] = data['close'][76] * 0.97
        data['close'][78] = data['close'][77] * 1.01
        data['close'][79] = data['close'][78] * 1.04
        data['close'][80] = data['close'][79] * 1.05

        # 각 봉의 high/low 업데이트
        for i in range(100):
            data['high'][i] = max(data['open'][i], data['close'][i]) + abs(np.random.normal(0, 50))
            data['low'][i] = min(data['open'][i], data['close'][i]) - abs(np.random.normal(0, 50))

        # 데이터프레임 생성
        df = pd.DataFrame(data)

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

        # RSI 계산
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # ATR 계산
        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - df['close'].shift(1))
        tr3 = abs(df['low'] - df['close'].shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=14).mean()
        df['atr_pct'] = df['atr'] / df['close'] * 100

        return df

    def test_identify_peaks_and_troughs(self):
        """피크와 트로프 식별 테스트"""
        peaks, troughs = self.analyzer.identify_peaks_and_troughs(self.sample_data)

        # 기본 검증
        self.assertIsInstance(peaks, np.ndarray)
        self.assertIsInstance(troughs, np.ndarray)

        # 최소한의 피크와 트로프가 식별되었는지 확인
        self.assertGreater(len(peaks), 0, "피크가 식별되지 않음")
        self.assertGreater(len(troughs), 0, "트로프가 식별되지 않음")

        print(f"식별된 피크: {len(peaks)}개, 트로프: {len(troughs)}개")

    def test_identify_support_resistance(self):
        """지지/저항 레벨 식별 테스트"""
        levels = self.analyzer.identify_support_resistance(self.sample_data)

        # 기본 검증
        self.assertIsInstance(levels, dict)
        self.assertIn('support', levels)
        self.assertIn('resistance', levels)

        # 지지/저항 레벨이 식별되었는지 확인
        self.assertTrue(any(levels['support']) or any(levels['resistance']),
                        "지지/저항 레벨이 식별되지 않음")

        print(f"식별된 지지 레벨: {len(levels['support'])}개, 저항 레벨: {len(levels['resistance'])}개")

        if levels['support']:
            print(f"첫 번째 지지 레벨: {levels['support'][0]}")
        if levels['resistance']:
            print(f"첫 번째 저항 레벨: {levels['resistance'][0]}")

    def test_identify_chart_patterns(self):
        """차트 패턴 식별 테스트"""
        patterns = self.analyzer.identify_chart_patterns(self.sample_data)

        # 기본 검증
        self.assertIsInstance(patterns, list)

        # 패턴이 식별되었는지 확인 (샘플 데이터에 패턴이 있다는 보장은 없음)
        print(f"식별된 차트 패턴: {len(patterns)}개")

        for i, pattern in enumerate(patterns[:3]):  # 최대 3개만 출력
            print(f"패턴 {i + 1}: {pattern.get('pattern_type')}, 신뢰도: {pattern.get('confidence', 0):.2f}%")

    def test_calculate_fibonacci_levels(self):
        """피보나치 레벨 계산 테스트"""
        high = 12000
        low = 10000

        # 상승 추세에서의 피보나치 레벨
        uptrend_levels = self.analyzer.calculate_fibonacci_levels(high, low, 'uptrend')

        # 기본 검증
        self.assertIsInstance(uptrend_levels, dict)
        self.assertEqual(uptrend_levels['0.0'], low)
        self.assertEqual(uptrend_levels['1.0'], high)

        # 하락 추세에서의 피보나치 레벨
        downtrend_levels = self.analyzer.calculate_fibonacci_levels(high, low, 'downtrend')

        # 기본 검증
        self.assertIsInstance(downtrend_levels, dict)
        self.assertEqual(downtrend_levels['0.0'], high)
        self.assertEqual(downtrend_levels['1.0'], low)

        print("피보나치 레벨 계산 테스트 통과")

    def test_analyze_chart(self):
        """차트 분석 통합 테스트"""
        results = self.analyzer.analyze_chart("BTC/USDT", "1d")

        # 기본 검증
        self.assertIsInstance(results, dict)
        self.assertEqual(results['symbol'], "BTC/USDT")
        self.assertEqual(results['timeframe'], "1d")
        self.assertIn('patterns', results)
        self.assertIn('support_resistance', results)
        self.assertIn('trend', results)

        print(f"차트 분석 결과: 추세={results['trend']}, 패턴 수={len(results['patterns'])}")

    def tearDown(self):
        """테스트 정리"""
        pass


if __name__ == "__main__":
    unittest.main()
