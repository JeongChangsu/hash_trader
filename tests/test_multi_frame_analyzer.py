# tests/test_multi_timeframe_analyzer.py
"""
다중 시간프레임 분석기 테스트 모듈

이 모듈은 multi_timeframe_analyzer.py의 기능을 테스트합니다.
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

from analyzers.multi_timeframe_analyzer import MultiTimeframeAnalyzer


class TestMultiTimeframeAnalyzer(unittest.TestCase):
    """다중 시간프레임 분석기 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        # 테스트 대상 인스턴스 생성
        self.analyzer = MultiTimeframeAnalyzer()

        # 샘플 분석 결과 생성
        self.sample_analysis = self.create_sample_analysis()

        # get_chart_analysis 메서드를 목(mock)으로 대체
        self.analyzer.get_chart_analysis = MagicMock(return_value=self.sample_analysis)

    def create_sample_analysis(self):
        """테스트용 샘플 차트 분석 결과 생성"""
        return {
            'symbol': 'BTC/USDT',
            'timeframe': '1d',
            'timestamp': int(datetime.now().timestamp() * 1000),
            'patterns': [
                {
                    'pattern_type': 'double_bottom',
                    'confidence': 75.5,
                    'target_price': 12000.0,
                    'stop_loss_price': 9500.0
                },
                {
                    'pattern_type': 'ascending_triangle',
                    'confidence': 65.2,
                    'target_price': 11500.0,
                    'stop_loss_price': 9800.0
                }
            ],
            'support_resistance': {
                'support': [
                    {'price': 10000.0, 'strength': 8.5},
                    {'price': 9500.0, 'strength': 7.2}
                ],
                'resistance': [
                    {'price': 11000.0, 'strength': 6.8},
                    {'price': 12000.0, 'strength': 5.5}
                ]
            },
            'trend': 'uptrend'
        }

    def test_timeframe_to_minutes(self):
        """시간프레임을 분 단위로 변환하는 테스트"""
        # 다양한 시간프레임에 대한 테스트
        self.assertEqual(self.analyzer.timeframe_to_minutes('1m'), 1)
        self.assertEqual(self.analyzer.timeframe_to_minutes('5m'), 5)
        self.assertEqual(self.analyzer.timeframe_to_minutes('15m'), 15)
        self.assertEqual(self.analyzer.timeframe_to_minutes('1h'), 60)
        self.assertEqual(self.analyzer.timeframe_to_minutes('4h'), 240)
        self.assertEqual(self.analyzer.timeframe_to_minutes('1d'), 60 * 24)
        self.assertEqual(self.analyzer.timeframe_to_minutes('1w'), 60 * 24 * 7)

        print("시간프레임 변환 테스트 통과")

    def test_calculate_timeframe_weights(self):
        """시간프레임 가중치 계산 테스트"""
        weights = self.analyzer.calculate_timeframe_weights()

        # 기본 검증
        self.assertIsInstance(weights, dict)
        self.assertTrue(all(0 < w < 1 for w in weights.values()), "가중치가 0-1 범위 밖에 있음")

        # 가중치 합이 1에 가까운지 확인
        total_weight = sum(weights.values())
        self.assertAlmostEqual(total_weight, 1.0, places=2, msg="가중치 합이 1이 아님")

        # HTF가 LTF보다 가중치가 높은지 확인
        timeframes = sorted(weights.keys(), key=self.analyzer.timeframe_to_minutes, reverse=True)
        for i in range(len(timeframes) - 1):
            self.assertGreaterEqual(
                weights[timeframes[i]],
                weights[timeframes[i + 1]],
                f"{timeframes[i]}의 가중치가 {timeframes[i + 1]}보다 작음"
            )

        print(f"시간프레임 가중치: {weights}")

    def test_extract_trend_signals(self):
        """추세 신호 추출 테스트"""
        signals = self.analyzer.extract_trend_signals(self.sample_analysis)

        # 기본 검증
        self.assertIsInstance(signals, dict)
        self.assertIn('trend', signals)
        self.assertIn('patterns', signals)
        self.assertIn('support', signals)
        self.assertIn('resistance', signals)

        # 추세 값이 분석 결과와 일치하는지 확인
        self.assertEqual(signals['trend'], self.sample_analysis['trend'])

        print(f"추출된 추세 신호: {signals['trend']}")
        print(f"패턴 수: {len(signals['patterns'])}")
        print(f"지지 레벨 수: {len(signals['support'])}")
        print(f"저항 레벨 수: {len(signals['resistance'])}")

    def test_merge_price_levels(self):
        """가격 레벨 병합 테스트"""
        levels = [
            {'price': 10000.0, 'strength': 5.0, 'timeframe': '1d'},
            {'price': 10050.0, 'strength': 3.0, 'timeframe': '4h'},  # 10000과 병합됨
            {'price': 11000.0, 'strength': 4.0, 'timeframe': '1d'},
            {'price': 9000.0, 'strength': 2.0, 'timeframe': '1h'}
        ]

        merged = self.analyzer.merge_price_levels(levels, 0.01)  # 1% 임계값

        # 기본 검증
        self.assertIsInstance(merged, list)

        # 10000과 10050이 병합되었는지 확인
        self.assertEqual(len(merged), 3, "레벨이 예상대로 병합되지 않음")

        # 병합된 레벨 확인
        merged_10k = None
        for level in merged:
            if 10000 <= level['price'] <= 10050:
                merged_10k = level
                break

        self.assertIsNotNone(merged_10k, "10000-10050 레벨이 병합되지 않음")
        self.assertIn('1d', merged_10k['timeframes'])
        self.assertIn('4h', merged_10k['timeframes'])
        self.assertEqual(merged_10k['source_count'], 2)

        print(f"병합된 가격 레벨: {merged}")

    @patch('analyzers.multi_timeframe_analyzer.redis.Redis')
    @patch('analyzers.multi_timeframe_analyzer.psycopg2.connect')
    def test_analyze_htf_to_ltf(self, mock_connect, mock_redis):
        """HTF→LTF 분석 테스트"""
        # 여러 시간프레임에 대한 분석 결과 설정
        tf_results = {
            '1w': {**self.sample_analysis, 'timeframe': '1w', 'trend': 'uptrend'},
            '1d': {**self.sample_analysis, 'timeframe': '1d', 'trend': 'uptrend'},
            '4h': {**self.sample_analysis, 'timeframe': '4h', 'trend': 'ranging'},
            '1h': {**self.sample_analysis, 'timeframe': '1h', 'trend': 'uptrend'}
        }

        # get_chart_analysis 재설정
        self.analyzer.get_chart_analysis = lambda symbol, tf: tf_results.get(tf, self.sample_analysis)

        # HTF→LTF 분석 실행
        result = self.analyzer.analyze_htf_to_ltf('BTC/USDT')

        # 기본 검증
        self.assertIsInstance(result, dict)
        self.assertEqual(result['symbol'], 'BTC/USDT')
        self.assertIn('htf_signals', result)
        self.assertIn('ltf_signals', result)
        self.assertIn('timeframe_coherence', result)
        self.assertIn('dominant_trend', result)
        self.assertIn('recommendations', result)

        # HTF 신호에 큰 시간프레임이 포함되어 있는지 확인
        self.assertIn('1w', result['htf_signals'])
        self.assertIn('1d', result['htf_signals'])

        # LTF 신호에 작은 시간프레임이 포함되어 있는지 확인
        self.assertIn('4h', result['ltf_signals'])
        self.assertIn('1h', result['ltf_signals'])

        print(f"HTF→LTF 분석 결과:")
        print(f"지배적 추세: {result['dominant_trend']}")
        print(f"시간프레임 정합성: {result['timeframe_coherence']:.2f}")
        print(f"추천 행동: {result['recommendations'].get('action', 'neutral')}")

    def tearDown(self):
        """테스트 정리"""
        # 설정된 모의(mock) 객체 정리
        self.analyzer.get_chart_analysis = None


if __name__ == "__main__":
    unittest.main()
