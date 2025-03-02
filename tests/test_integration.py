# tests/test_integration.py
"""
통합 테스트 모듈

이 모듈은 여러 모듈 간의 통합 기능을 테스트합니다.
데이터 수집부터 전략 선택까지의 전체 파이프라인을 검증합니다.
"""

import sys
import os
import unittest
import asyncio
from unittest.mock import MagicMock, patch

# 상위 디렉토리를 sys.path에 추가하여 모듈 임포트 가능하게 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzers.chart_pattern_analyzer import ChartPatternAnalyzer
from analyzers.multi_timeframe_analyzer import MultiTimeframeAnalyzer
from analyzers.market_condition_classifier import MarketConditionClassifier
from analyzers.ai_strategy_selector import AIStrategySelector


class TestIntegration(unittest.TestCase):
    """통합 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        # 테스트 대상 인스턴스 생성
        self.chart_analyzer = ChartPatternAnalyzer()
        self.mtf_analyzer = MultiTimeframeAnalyzer()
        self.market_classifier = MarketConditionClassifier()
        self.strategy_selector = AIStrategySelector()

        # 기본 테스트 심볼
        self.test_symbol = "BTC/USDT"

        # 각 데이터베이스 연결을 모의 객체로 대체
        self.mock_db_connections()

        # 차트 분석 캐싱
        self.chart_analysis_cache = {}

    def mock_db_connections(self):
        """데이터베이스 연결을 모의 객체로 대체"""
        # Redis 및 PostgreSQL 연결 모의 설정
        for analyzer in [self.chart_analyzer, self.mtf_analyzer, self.market_classifier, self.strategy_selector]:
            analyzer.redis_client = MagicMock()
            analyzer.db_conn = MagicMock()

            # 커서 모의 설정
            mock_cursor = MagicMock()
            analyzer.db_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []

    @patch.object(ChartPatternAnalyzer, 'fetch_ohlcv_data')
    @patch.object(MultiTimeframeAnalyzer, 'get_chart_analysis')
    @patch.object(MarketConditionClassifier, 'fetch_multi_timeframe_data')
    @patch.object(AIStrategySelector, 'get_market_condition')
    @patch.object(AIStrategySelector, 'get_ai_strategy_recommendation')
    async def test_full_pipeline(self, mock_get_ai, mock_get_market, mock_fetch_mtf, mock_get_chart, mock_fetch_ohlcv):
        """전체 파이프라인 테스트"""
        # 테스트 시나리오: 상승 추세 시장에서의 분석 및 전략 선택

        # 1. 차트 패턴 분석
        # 모의 OHLCV 데이터 설정
        mock_ohlcv_df = self.create_mock_ohlcv_data('uptrend')
        mock_fetch_ohlcv.return_value = mock_ohlcv_df

        # 차트 분석 실행
        chart_analysis = self.chart_analyzer.analyze_chart(self.test_symbol, "1d")

        print(f"1. 차트 패턴 분석 완료:")
        print(f"   추세: {chart_analysis['trend']}")
        print(f"   패턴 수: {len(chart_analysis['patterns'])}")
        print(f"   지지 레벨 수: {len(chart_analysis['support_resistance']['support'])}")

        # 차트 분석 결과 캐싱
        self.chart_analysis_cache['1d'] = chart_analysis

        # 2. 다중 시간프레임 분석
        # 차트 분석 함수 모의 설정
        mock_get_chart.side_effect = lambda symbol, tf: self.chart_analysis_cache.get(tf, chart_analysis)

        # 다중 시간프레임 분석 실행
        mtf_analysis = self.mtf_analyzer.analyze_htf_to_ltf(self.test_symbol)

        print(f"\n2. 다중 시간프레임 분석 완료:")
        print(f"   지배적 추세: {mtf_analysis['dominant_trend']}")
        print(f"   시간프레임 정합성: {mtf_analysis['timeframe_coherence']:.2f}")
        print(f"   추천 행동: {mtf_analysis['recommendations'].get('action', 'neutral')}")

        # 3. 시장 상황 분류
        # 다중 시간프레임 데이터 모의 설정
        mock_mtf_data = {
            '1d': mock_ohlcv_df,
            '4h': mock_ohlcv_df,
            '1h': mock_ohlcv_df
        }
        mock_fetch_mtf.return_value = mock_mtf_data

        # 시장 상황 분류 실행
        market_condition = self.market_classifier.classify_market_condition(mock_mtf_data)

        print(f"\n3. 시장 상황 분류 완료:")
        print(f"   주요 상황: {market_condition['primary_condition']}")
        print(f"   보조 상황: {market_condition['secondary_condition']}")
        print(f"   시장 국면: {market_condition['market_phase']}")
        print(f"   추천 전략: {market_condition['recommended_strategies']}")

        # 4. AI 전략 선택
        # 시장 상황 함수 모의 설정
        mock_get_market.return_value = market_condition

        # AI 추천 모의 설정
        mock_ai_recommendation = {
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
        mock_get_ai.return_value = mock_ai_recommendation

        # 최적 전략 선택 실행
        strategy_result = await self.strategy_selector.select_optimal_strategy(self.test_symbol)

        print(f"\n4. AI 전략 선택 완료:")
        print(f"   AI 추천 주요 전략: {strategy_result['ai_recommendation']['primary_strategy']}")
        print(f"   AI 추천 보조 전략: {strategy_result['ai_recommendation']['secondary_strategy']}")
        print(f"   신뢰도: {strategy_result['ai_recommendation']['confidence']:.2f}%")

        # 5. 전체 파이프라인 검증
        self.assertEqual(chart_analysis['trend'], 'uptrend', "차트 분석 추세가 예상과 다름")
        self.assertEqual(mtf_analysis['dominant_trend'], 'uptrend', "다중 시간프레임 분석 추세가 예상과 다름")
        self.assertEqual(market_condition['primary_condition'], 'trending_up', "시장 상황이 예상과 다름")
        self.assertEqual(strategy_result['ai_recommendation']['primary_strategy'], 'trend_following', "추천 전략이 예상과 다름")

        print(f"\n✅ 전체 파이프라인 테스트 완료 - 모든 단계가 예상대로 작동하였습니다.")

    def create_mock_ohlcv_data(self, trend_type='uptrend'):
        """모의 OHLCV 데이터 생성"""
        # 실제 구현은 다른 테스트 클래스에서 차용할 수 있음
        # 여기서는 간단한 MagicMock 객체 반환
        mock_df = MagicMock()

        # 추세 유형에 따른 주요 속성 설정
        if trend_type == 'uptrend':
            mock_df.trend = 'uptrend'
            mock_df.direction = 'up'
        elif trend_type == 'downtrend':
            mock_df.trend = 'downtrend'
            mock_df.direction = 'down'
        else:
            mock_df.trend = 'ranging'
            mock_df.direction = 'neutral'

        return mock_df

    def tearDown(self):
        """테스트 정리"""
        pass


# 비동기 테스트를 위한 실행 함수
def run_async_test(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


if __name__ == "__main__":
    # 비동기 테스트를 위한 실행 방식 변경
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIntegration)
    for test in suite:
        if test._testMethodName.startswith('test_') and asyncio.iscoroutinefunction(
                getattr(TestIntegration, test._testMethodName)):
            test_method = getattr(TestIntegration, test._testMethodName)
            setattr(TestIntegration, test._testMethodName,
                    lambda self, method=test_method: run_async_test(method(self)))

    unittest.main()
