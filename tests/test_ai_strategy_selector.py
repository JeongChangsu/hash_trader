# tests/test_ai_strategy_selector.py
"""
AI 전략 선택기 테스트 모듈

이 모듈은 ai_strategy_selector.py의 기능을 테스트합니다.
"""

import sys
import os
import unittest
import json
import asyncio
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock

# 상위 디렉토리를 sys.path에 추가하여 모듈 임포트 가능하게 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzers.ai_strategy_selector import AIStrategySelector


class TestAIStrategySelector(unittest.TestCase):
    """AI 전략 선택기 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        # 테스트 대상 인스턴스 생성
        self.selector = AIStrategySelector()

        # 샘플 시장 상황 데이터 생성
        self.sample_market_condition = self.create_sample_market_condition()

        # 샘플 다중 시간프레임 분석 결과 생성
        self.sample_mtf_analysis = self.create_sample_mtf_analysis()

        # 샘플 차트 패턴 생성
        self.sample_chart_patterns = self.create_sample_chart_patterns()

        # 모의 객체 설정
        self.selector.get_market_condition = MagicMock(return_value=self.sample_market_condition)
        self.selector.get_multi_timeframe_analysis = MagicMock(return_value=self.sample_mtf_analysis)
        self.selector.get_chart_patterns = MagicMock(return_value=self.sample_chart_patterns)
        self.selector.get_current_price = MagicMock(return_value=10500.0)

    def create_sample_market_condition(self):
        """샘플 시장 상황 데이터 생성"""
        return {
            'primary_condition': 'trending_up',
            'secondary_condition': 'breakout',
            'market_phase': 'advancing',
            'volatility': {
                'score': 3.5,
                'level': 'medium'
            },
            'trend': {
                'strength': 1.8,
                'direction': 'up'
            },
            'confidence': 85.0,
            'recommended_strategies': ['trend_following', 'breakout'],
            'data_sources': {
                'timeframes_analyzed': ['1h', '4h', '1d', '1w']
            }
        }

    def create_sample_mtf_analysis(self):
        """샘플 다중 시간프레임 분석 결과 생성"""
        return {
            'timeframe_coherence': 0.85,
            'dominant_trend': 'uptrend',
            'htf_signals': {
                '1d': {
                    'trend': 'uptrend',
                    'patterns': [{'type': 'double_bottom', 'confidence': 75.5}]
                },
                '1w': {
                    'trend': 'uptrend',
                    'patterns': []
                }
            },
            'ltf_signals': {
                '4h': {
                    'trend': 'uptrend',
                    'patterns': [{'type': 'ascending_triangle', 'confidence': 65.2}]
                },
                '1h': {
                    'trend': 'ranging',
                    'patterns': []
                }
            },
            'recommendations': {
                'action': 'buy',
                'target_price': 11500.0,
                'stop_loss_price': 9800.0
            }
        }

    def create_sample_chart_patterns(self):
        """샘플 차트 패턴 생성"""
        return [
            {
                'timeframe': '1d',
                'pattern_type': 'double_bottom',
                'data': {
                    'confidence': 75.5,
                    'target_price': 12000.0,
                    'stop_loss_price': 9500.0
                },
                'confidence': 75.5
            },
            {
                'timeframe': '4h',
                'pattern_type': 'ascending_triangle',
                'data': {
                    'confidence': 65.2,
                    'target_price': 11500.0,
                    'stop_loss_price': 9800.0
                },
                'confidence': 65.2
            }
        ]

    def test_select_weighted_strategies(self):
        """가중치 기반 전략 선택 테스트"""
        # 전략 선택 실행
        strategies = self.selector.select_weighted_strategies(self.sample_market_condition)

        # 기본 검증
        self.assertIsInstance(strategies, list)
        self.assertGreater(len(strategies), 0, "선택된 전략이 없음")

        # 올바른 정렬 확인
        for i in range(len(strategies) - 1):
            self.assertGreaterEqual(
                strategies[i]["score"],
                strategies[i + 1]["score"],
                "전략이 점수 기준으로 내림차순 정렬되지 않음"
            )

        # 추세추종 전략이 상위 전략에 포함되어 있는지 확인 (상승 추세 시장 조건)
        top_strategy_ids = [s["id"] for s in strategies[:2]]
        self.assertIn('trend_following', top_strategy_ids, "상승 추세 상황에서 추세추종 전략이 상위에 없음")

        print(f"선택된 전략:")
        for i, strategy in enumerate(strategies):
            print(f"{i + 1}. {strategy['name']} (ID: {strategy['id']}): 점수 {strategy['score']:.2f}")

    def test_evaluate_strategy_historical_performance(self):
        """전략 과거 성능 평가 테스트"""
        # 성능 평가 실행
        performance = self.selector.evaluate_strategy_historical_performance('trend_following', 'trending_up')

        # 기본 검증
        self.assertIsInstance(performance, dict)
        self.assertEqual(performance['strategy_id'], 'trend_following')
        self.assertEqual(performance['market_condition'], 'trending_up')
        self.assertIn('win_rate', performance)
        self.assertIn('profit_factor', performance)
        self.assertIn('avg_trade_pct', performance)

        print(f"전략 과거 성능:")
        print(f"전략: {performance['strategy_id']}")
        print(f"시장 상황: {performance['market_condition']}")
        print(f"승률: {performance['win_rate']:.2f}")
        print(f"손익비: {performance['profit_factor']:.2f}")
        print(f"평균 수익률: {performance['avg_trade_pct']:.2f}%")

    @patch('analyzers.ai_strategy_selector.genai')
    @patch('analyzers.ai_strategy_selector.anthropic')
    async def test_get_ai_strategy_recommendation(self, mock_anthropic, mock_genai):
        """AI 전략 추천 테스트"""
        # AI API 모의 응답 설정
        mock_gemini_response = MagicMock()
        mock_gemini_response.text = json.dumps({
            "primary_strategy": "trend_following",
            "secondary_strategy": "breakout",
            "confidence": 85.0,
            "reasoning": "Strong uptrend across multiple timeframes with HTF confirmation.",
            "entry_criteria": ["Entry on pullbacks to 20EMA", "Confirm with volume increase"],
            "tp_levels": [11500.0, 12000.0],
            "sl_level": 9800.0,
            "ranking": [
                {"strategy": "trend_following", "score": 0.85, "reasoning": "Strong uptrend"},
                {"strategy": "breakout", "score": 0.75, "reasoning": "Recent breakout pattern"}
            ]
        })

        mock_genai.Client.return_value.models.generate_content.return_value = mock_gemini_response

        # 이미 설정된 모의 객체로 get_current_price를 사용하도록 함

        # AI 전략 추천 실행
        recommendation = await self.selector.get_ai_strategy_recommendation('BTC/USDT')

        # 기본 검증
        self.assertIsInstance(recommendation, dict)
        self.assertIn('primary_strategy', recommendation)
        self.assertIn('secondary_strategy', recommendation)
        self.assertIn('confidence', recommendation)
        self.assertIn('reasoning', recommendation)
        self.assertIn('entry_criteria', recommendation)
        self.assertIn('tp_levels', recommendation)
        self.assertIn('sl_level', recommendation)

        print(f"AI 전략 추천:")
        print(f"주요 전략: {recommendation['primary_strategy']}")
        print(f"보조 전략: {recommendation['secondary_strategy']}")
        print(f"신뢰도: {recommendation['confidence']:.2f}%")
        print(f"근거: {recommendation['reasoning']}")
        print(f"진입 기준: {recommendation['entry_criteria']}")
        print(f"TP 레벨: {recommendation['tp_levels']}")
        print(f"SL 레벨: {recommendation['sl_level']}")

    @patch('analyzers.ai_strategy_selector.redis.Redis')
    @patch('analyzers.ai_strategy_selector.psycopg2.connect')
    @patch.object(AIStrategySelector, 'get_ai_strategy_recommendation')
    async def test_select_optimal_strategy(self, mock_get_ai, mock_connect, mock_redis):
        """최적 전략 선택 테스트"""
        # AI 추천 모의 응답 설정
        mock_get_ai.return_value = {
            "primary_strategy": "trend_following",
            "secondary_strategy": "breakout",
            "confidence": 85.0,
            "reasoning": "Strong uptrend across multiple timeframes with HTF confirmation.",
            "entry_criteria": ["Entry on pullbacks to 20EMA", "Confirm with volume increase"],
            "tp_levels": [11500.0, 12000.0],
            "sl_level": 9800.0,
            "ranking": [
                {"strategy": "trend_following", "score": 0.85, "reasoning": "Strong uptrend"},
                {"strategy": "breakout", "score": 0.75, "reasoning": "Recent breakout pattern"}
            ]
        }

        # 최적 전략 선택 실행
        result = await self.selector.select_optimal_strategy('BTC/USDT')

        # 기본 검증
        self.assertIsInstance(result, dict)
        self.assertEqual(result['symbol'], 'BTC/USDT')
        self.assertIn('ai_recommendation', result)
        self.assertIn('weighted_strategies', result)
        self.assertIn('market_condition', result)

        ai_rec = result['ai_recommendation']
        self.assertEqual(ai_rec['primary_strategy'], 'trend_following')

        print(f"최적 전략 선택 결과:")
        print(f"심볼: {result['symbol']}")
        print(f"AI 추천 주요 전략: {ai_rec['primary_strategy']}")
        print(f"AI 추천 보조 전략: {ai_rec['secondary_strategy']}")
        print(f"가중치 선택 상위 전략: {result['weighted_strategies'][0]['id']}")

    def tearDown(self):
        """테스트 정리"""
        # 설정된 모의(mock) 객체 정리
        self.selector.get_market_condition = None
        self.selector.get_multi_timeframe_analysis = None
        self.selector.get_chart_patterns = None


# 비동기 테스트를 위한 실행 함수
def run_async_test(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


if __name__ == "__main__":
    unittest.main()
