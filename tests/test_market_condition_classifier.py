# tests/test_market_condition_classifier.py
"""
시장 상황 분류기 테스트 모듈

이 모듈은 market_condition_classifier.py의 기능을 테스트합니다.
"""

import sys
import os
import unittest
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# 상위 디렉토리를 sys.path에 추가하여 모듈 임포트 가능하게 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzers.market_condition_classifier import MarketConditionClassifier


class TestMarketConditionClassifier(unittest.TestCase):
    """시장 상황 분류기 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        # 테스트 대상 인스턴스 생성
        self.classifier = MarketConditionClassifier()

        # 샘플 OHLCV 데이터 생성
        self.uptrend_df = self.create_uptrend_data()
        self.downtrend_df = self.create_downtrend_data()
        self.ranging_df = self.create_ranging_data()

        # 모의 객체 설정
        self.classifier.fetch_ohlcv_data = MagicMock(return_value=self.uptrend_df)

    def create_uptrend_data(self):
        """상승 추세 샘플 데이터 생성"""
        # 날짜 범위 생성
        dates = [datetime.now() - timedelta(days=i) for i in range(100, 0, -1)]

        # 기본 데이터 생성
        data = {
            'timestamp': dates,
            'open': np.zeros(100),
            'high': np.zeros(100),
            'low': np.zeros(100),
            'close': np.zeros(100),
            'volume': np.random.normal(100, 30, 100)
        }

        # 상승 추세 생성
        base_price = 10000
        for i in range(100):
            # 매일 평균 0.5% 상승 + 무작위 변동
            daily_return = 0.005 + np.random.normal(0, 0.01)
            if i == 0:
                data['close'][i] = base_price
            else:
                data['close'][i] = data['close'][i - 1] * (1 + daily_return)

            data['open'][i] = data['close'][i] * (1 - np.random.uniform(0, 0.01))
            data['high'][i] = max(data['open'][i], data['close'][i]) * (1 + np.random.uniform(0, 0.01))
            data['low'][i] = min(data['open'][i], data['close'][i]) * (1 - np.random.uniform(0, 0.01))

        # 데이터프레임 생성
        df = pd.DataFrame(data)

        # 필요한 모든 기술적 지표 계산
        self.calculate_indicators(df)

        return df

    def create_downtrend_data(self):
        """하락 추세 샘플 데이터 생성"""
        # 날짜 범위 생성
        dates = [datetime.now() - timedelta(days=i) for i in range(100, 0, -1)]

        # 기본 데이터 생성
        data = {
            'timestamp': dates,
            'open': np.zeros(100),
            'high': np.zeros(100),
            'low': np.zeros(100),
            'close': np.zeros(100),
            'volume': np.random.normal(100, 30, 100)
        }

        # 하락 추세 생성
        base_price = 10000
        for i in range(100):
            # 매일 평균 0.5% 하락 + 무작위 변동
            daily_return = -0.005 + np.random.normal(0, 0.01)
            if i == 0:
                data['close'][i] = base_price
            else:
                data['close'][i] = data['close'][i - 1] * (1 + daily_return)

            data['open'][i] = data['close'][i] * (1 + np.random.uniform(0, 0.01))
            data['high'][i] = max(data['open'][i], data['close'][i]) * (1 + np.random.uniform(0, 0.01))
            data['low'][i] = min(data['open'][i], data['close'][i]) * (1 - np.random.uniform(0, 0.01))

        # 데이터프레임 생성
        df = pd.DataFrame(data)

        # 필요한 모든 기술적 지표 계산
        self.calculate_indicators(df)

        return df

    def create_ranging_data(self):
        """횡보 추세 샘플 데이터 생성"""
        # 날짜 범위 생성
        dates = [datetime.now() - timedelta(days=i) for i in range(100, 0, -1)]

        # 기본 데이터 생성
        data = {
            'timestamp': dates,
            'open': np.zeros(100),
            'high': np.zeros(100),
            'low': np.zeros(100),
            'close': np.zeros(100),
            'volume': np.random.normal(100, 30, 100)
        }

        # 횡보 추세 생성
        base_price = 10000
        for i in range(100):
            # 랜덤 변동만 있는 횡보 패턴
            if i == 0:
                data['close'][i] = base_price
            else:
                data['close'][i] = data['close'][i - 1] * (1 + np.random.normal(0, 0.005))

            data['open'][i] = data['close'][i] * (1 + np.random.uniform(-0.005, 0.005))
            data['high'][i] = max(data['open'][i], data['close'][i]) * (1 + np.random.uniform(0, 0.005))
            data['low'][i] = min(data['open'][i], data['close'][i]) * (1 - np.random.uniform(0, 0.005))

        # 데이터프레임 생성
        df = pd.DataFrame(data)

        # 필요한 모든 기술적 지표 계산
        self.calculate_indicators(df)

        return df

    def calculate_indicators(self, df):
        """기술적 지표 계산"""
        # 가격 변화율
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

    def test_calculate_market_volatility(self):
        """시장 변동성 계산 테스트"""
        # 상승 추세 데이터로 테스트
        uptrend_volatility, uptrend_level = self.classifier.calculate_market_volatility(self.uptrend_df)

        # 기본 검증
        self.assertIsInstance(uptrend_volatility, float)
        self.assertIsInstance(uptrend_level, str)
        self.assertGreater(uptrend_volatility, 0, "변동성이 0보다 작거나 같음")

        # 하락 추세 데이터로 테스트
        self.classifier.fetch_ohlcv_data = MagicMock(return_value=self.downtrend_df)
        downtrend_volatility, downtrend_level = self.classifier.calculate_market_volatility(self.downtrend_df)

        # 횡보 추세 데이터로 테스트
        self.classifier.fetch_ohlcv_data = MagicMock(return_value=self.ranging_df)
        ranging_volatility, ranging_level = self.classifier.calculate_market_volatility(self.ranging_df)

        print(f"상승 추세 변동성: {uptrend_volatility:.2f} ({uptrend_level})")
        print(f"하락 추세 변동성: {downtrend_volatility:.2f} ({downtrend_level})")
        print(f"횡보 추세 변동성: {ranging_volatility:.2f} ({ranging_level})")

    def test_calculate_trend_strength(self):
        """추세 강도 계산 테스트"""
        # 상승 추세 데이터로 테스트
        uptrend_strength, uptrend_direction = self.classifier.calculate_trend_strength(self.uptrend_df)

        # 기본 검증
        self.assertIsInstance(uptrend_strength, float)
        self.assertIsInstance(uptrend_direction, str)
        self.assertIn(uptrend_direction, ['strongly_up', 'up', 'neutral', 'down', 'strongly_down'])

        # 하락 추세 데이터로 테스트
        downtrend_strength, downtrend_direction = self.classifier.calculate_trend_strength(self.downtrend_df)

        # 횡보 추세 데이터로 테스트
        ranging_strength, ranging_direction = self.classifier.calculate_trend_strength(self.ranging_df)

        print(f"상승 추세 강도: {uptrend_strength:.2f} ({uptrend_direction})")
        print(f"하락 추세 강도: {downtrend_strength:.2f} ({downtrend_direction})")
        print(f"횡보 추세 강도: {ranging_strength:.2f} ({ranging_direction})")

        # 추세 방향이 데이터와 일치하는지 확인
        self.assertIn(uptrend_direction, ['up', 'strongly_up'], "상승 추세가 올바르게 감지되지 않음")
        self.assertIn(downtrend_direction, ['down', 'strongly_down'], "하락 추세가 올바르게 감지되지 않음")

    def test_detect_market_phases(self):
        """시장 국면 감지 테스트"""
        # 상승 추세 데이터로 테스트
        uptrend_phase = self.classifier.detect_market_phases(self.uptrend_df)

        # 기본 검증
        self.assertIsInstance(uptrend_phase, str)

        # 하락 추세 데이터로 테스트
        downtrend_phase = self.classifier.detect_market_phases(self.downtrend_df)

        # 횡보 추세 데이터로 테스트
        ranging_phase = self.classifier.detect_market_phases(self.ranging_df)

        print(f"상승 추세 국면: {uptrend_phase}")
        print(f"하락 추세 국면: {downtrend_phase}")
        print(f"횡보 추세 국면: {ranging_phase}")

    @patch('analyzers.market_condition_classifier.redis.Redis')
    @patch('analyzers.market_condition_classifier.psycopg2.connect')
    def test_classify_market_condition(self, mock_connect, mock_redis):
        """시장 상황 분류 테스트"""
        # 시간프레임별 데이터 설정
        market_data = {
            '1d': self.uptrend_df,
            '4h': self.uptrend_df,
            '1h': self.ranging_df
        }

        # Onchain 데이터 모의 객체 설정
        onchain_data = {
            'market_analysis': {
                'market_sentiment': 'bullish'
            },
            'data': {
                'Exchange Reserve': {'signal': 'bullish'},
                'Exchange Netflow': {'signal': 'very_bullish'},
                'Adjusted SOPR': {'signal': 'neutral'}
            }
        }

        # 분류 실행
        result = self.classifier.classify_market_condition(market_data, onchain_data)

        # 기본 검증
        self.assertIsInstance(result, dict)
        self.assertIn('primary_condition', result)
        self.assertIn('secondary_condition', result)
        self.assertIn('market_phase', result)
        self.assertIn('volatility', result)
        self.assertIn('trend', result)
        self.assertIn('confidence', result)
        self.assertIn('recommended_strategies', result)

        print(f"시장 상황 분류 결과:")
        print(f"주요 상황: {result['primary_condition']}")
        print(f"보조 상황: {result['secondary_condition']}")
        print(f"시장 국면: {result['market_phase']}")
        print(f"추천 전략: {result['recommended_strategies']}")
        print(f"신뢰도: {result['confidence']:.2f}%")

    def test_analyze_onchain_signals(self):
        """온체인 신호 분석 테스트"""
        # 샘플 온체인 데이터
        onchain_data = {
            'data': {
                'Exchange Reserve': {'signal': 'bullish', 'latest_value': 2500000},
                'Exchange Netflow': {'signal': 'very_bullish', 'latest_value': -5000},
                'Adjusted SOPR': {'signal': 'neutral', 'latest_value': 1.01},
                'MVRV Ratio': {'signal': 'slightly_bearish', 'latest_value': 2.5},
                'Exchange Whale Ratio': {'signal': 'bearish', 'latest_value': 0.85}
            }
        }

        # 분석 실행
        result = self.classifier.analyze_onchain_signals(onchain_data)

        # 기본 검증
        self.assertIsInstance(result, dict)
        self.assertIn('bullish_signals', result)
        self.assertIn('bearish_signals', result)
        self.assertIn('neutral_signals', result)
        self.assertIn('overall_sentiment', result)

        print(f"온체인 신호 분석 결과:")
        print(f"전체 분위기: {result['overall_sentiment']}")
        print(f"강세 신호 수: {len(result['bullish_signals'])}")
        print(f"약세 신호 수: {len(result['bearish_signals'])}")
        print(f"중립 신호 수: {len(result['neutral_signals'])}")

    def tearDown(self):
        """테스트 정리"""
        # 설정된 모의(mock) 객체 정리
        self.classifier.fetch_ohlcv_data = None


if __name__ == "__main__":
    unittest.main()
