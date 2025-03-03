# analyzers/ai_strategy_selector.py
"""
AI 전략 선택기

이 모듈은 시장 데이터와 분석 결과를 기반으로 현재 상황에
최적화된 전략을 선택합니다. Gemini/Claude API를 활용하여
다양한 시장 상황에 대한 전략 추천을 제공합니다.
"""

import asyncio
import logging
import json
import redis
import psycopg2
import time
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta
from google import genai
import anthropic
import random

from config.settings import (
    POSTGRES_CONFIG,
    REDIS_CONFIG,
    GEMINI_API_KEY,
    CLAUDE_API_KEY,
    STRATEGY_WEIGHTS
)
from config.logging_config import configure_logging


class AIStrategySelector:
    """
    AI를 활용하여 최적의 트레이딩 전략을 선택하는 클래스
    """

    def __init__(self):
        """AIStrategySelector 초기화"""
        self.logger = configure_logging("ai_strategy_selector")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # Gemini API 초기화
        self.gemini_client = genai.Client(api_key=GEMINI_API_KEY)

        # Claude API 초기화
        self.claude_client = anthropic.Client(api_key=CLAUDE_API_KEY)

        # 지원하는 전략 목록
        self.supported_strategies = {
            "trend_following": {
                "name": "추세추종",
                "description": "중장기 추세 방향으로 포지션을 진입하여 추세가 지속되는 동안 유지하는 전략",
                "suitable_conditions": ["trending_up", "trending_down", "breakout", "breakdown"],
                "default_weight": STRATEGY_WEIGHTS.get("trend_following", 0.35)
            },
            "reversal": {
                "name": "추세반전",
                "description": "과매수/과매도 상태에서 추세 반전을 예측하여 역방향으로 포지션을 잡는 전략",
                "suitable_conditions": ["euphoria", "capitulation", "recovery", "correction"],
                "default_weight": STRATEGY_WEIGHTS.get("reversal", 0.25)
            },
            "breakout": {
                "name": "돌파",
                "description": "중요 레벨(지지/저항)을 돌파할 때 돌파 방향으로 포지션을 잡는 전략",
                "suitable_conditions": ["breakout", "consolidation", "accumulation"],
                "default_weight": STRATEGY_WEIGHTS.get("breakout", 0.25)
            },
            "range": {
                "name": "레인지",
                "description": "가격이 일정 범위 내에서 움직일 때 상/하단에서 반대 방향으로 포지션을 잡는 전략",
                "suitable_conditions": ["ranging", "consolidation", "low_volatility"],
                "default_weight": STRATEGY_WEIGHTS.get("range", 0.15)
            }
        }

        self.initialize_db()
        self.logger.info("AIStrategySelector 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ai_strategy_recommendations (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    primary_strategy VARCHAR(50) NOT NULL,
                    secondary_strategy VARCHAR(50),
                    strategies_ranking JSONB NOT NULL,
                    market_condition JSONB NOT NULL,
                    confidence FLOAT NOT NULL,
                    entry_points JSONB,
                    tp_sl_recommendations JSONB,
                    ai_reasoning TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_ai_strategy_lookup 
                ON ai_strategy_recommendations (symbol, timestamp_ms);
            """)
            self.db_conn.commit()
            self.logger.info("AI 전략 추천 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def get_market_condition(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        최신 시장 상황 분류 결과를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Optional[Dict]: 시장 상황 또는 None
        """
        self.logger.info(f"시장 상황 가져오는 중: {symbol}")

        # Redis에서 최신 시장 상황 데이터 가져오기
        redis_key = f"analysis:market_condition:{symbol.replace('/', '_')}"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error(f"시장 상황 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    primary_condition, secondary_condition, market_phase,
                    volatility_level, trend_strength, recommended_strategies,
                    classification_confidence, data_sources
                FROM 
                    market_conditions
                WHERE 
                    symbol = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """,
                (symbol,)
            )

            row = cursor.fetchone()
            if row:
                return {
                    'primary_condition': row[0],
                    'secondary_condition': row[1],
                    'market_phase': row[2],
                    'volatility': {'level': row[3]},
                    'trend': {'strength': row[4]},
                    'recommended_strategies': json.loads(row[5]),
                    'confidence': row[6],
                    'data_sources': json.loads(row[7])
                }
            else:
                self.logger.warning(f"시장 상황 데이터가 없습니다: {symbol}")
                return None

        except Exception as e:
            self.logger.error(f"시장 상황 가져오기 실패: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_multi_timeframe_analysis(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        최신 다중 시간프레임 분석 결과를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Optional[Dict]: 다중 시간프레임 분석 또는 None
        """
        self.logger.info(f"다중 시간프레임 분석 가져오는 중: {symbol}")

        # Redis에서 최신 분석 데이터 가져오기
        redis_key = f"analysis:multi_tf:{symbol.replace('/', '_')}"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error(f"다중 시간프레임 분석 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    tf_coherence, dominant_trend, htf_signals, ltf_signals, recommendations
                FROM 
                    multi_timeframe_analysis
                WHERE 
                    symbol = %s
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """,
                (symbol,)
            )

            row = cursor.fetchone()
            if row:
                return {
                    'timeframe_coherence': row[0],
                    'dominant_trend': row[1],
                    'htf_signals': json.loads(row[2]),
                    'ltf_signals': json.loads(row[3]),
                    'recommendations': json.loads(row[4])
                }
            else:
                self.logger.warning(f"다중 시간프레임 분석 데이터가 없습니다: {symbol}")
                return None

        except Exception as e:
            self.logger.error(f"다중 시간프레임 분석 가져오기 실패: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_chart_patterns(self, symbol: str) -> List[Dict[str, Any]]:
        """
        최근 차트 패턴을 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            List[Dict]: 차트 패턴 목록
        """
        self.logger.info(f"차트 패턴 가져오는 중: {symbol}")

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    timeframe, pattern_type, pattern_data, confidence
                FROM 
                    chart_patterns
                WHERE 
                    symbol = %s AND
                    created_at > (NOW() - INTERVAL '1 day')
                ORDER BY 
                    confidence DESC
                LIMIT 10
                """,
                (symbol,)
            )

            patterns = []
            for row in cursor.fetchall():
                try:
                    pattern_data = row[2] if isinstance(row[2], dict) else json.loads(row[2])
                    patterns.append({
                        'timeframe': row[0],
                        'pattern_type': row[1],
                        'data': pattern_data,
                        'confidence': row[3]
                    })
                except json.JSONDecodeError:
                    self.logger.error(f"패턴 데이터 JSON 파싱 오류")

            return patterns

        except Exception as e:
            self.logger.error(f"차트 패턴 가져오기 실패: {e}")
            return []
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_liquidation_clusters(self, symbol: str) -> Dict[str, Any]:
        """
        최신 청산 클러스터 데이터를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 청산 클러스터 데이터
        """
        self.logger.info(f"청산 클러스터 가져오는 중: {symbol}")

        # Redis에서 최신 데이터 가져오기
        redis_key = "data:liquidation_heatmap:latest"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error(f"청산 클러스터 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    data
                FROM 
                    liquidation_heatmap
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            else:
                self.logger.warning("청산 클러스터 데이터가 없습니다")
                return {}

        except Exception as e:
            self.logger.error(f"청산 클러스터 가져오기 실패: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_onchain_data(self) -> Dict[str, Any]:
        """
        최신 온체인 데이터를 가져옵니다.

        Returns:
            Dict: 온체인 데이터
        """
        self.logger.info("온체인 데이터 가져오는 중")

        # Redis에서 최신 데이터 가져오기
        redis_key = "data:onchain:latest"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error("온체인 데이터 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    data
                FROM 
                    onchain_data
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            else:
                self.logger.warning("온체인 데이터가 없습니다")
                return {}

        except Exception as e:
            self.logger.error(f"온체인 데이터 가져오기 실패: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_fear_greed_data(self) -> Dict[str, Any]:
        """
        최신 공포&탐욕 지수 데이터를 가져옵니다.

        Returns:
            Dict: 공포&탐욕 지수 데이터
        """
        self.logger.info("공포&탐욕 지수 가져오는 중")

        # Redis에서 최신 데이터 가져오기
        redis_key = "data:fear_greed:latest"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                return json.loads(cached_data)
            except json.JSONDecodeError:
                self.logger.error("공포&탐욕 지수 JSON 파싱 오류")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    fear_greed_index, sentiment_category, signal
                FROM 
                    fear_greed_index
                ORDER BY 
                    timestamp_ms DESC
                LIMIT 1
                """
            )

            row = cursor.fetchone()
            if row:
                return {
                    'fear_greed_index': row[0],
                    'sentiment_category': row[1],
                    'signal': row[2]
                }
            else:
                self.logger.warning("공포&탐욕 데이터가 없습니다")
                return {}

        except Exception as e:
            self.logger.error(f"공포&탐욕 데이터 가져오기 실패: {e}")
            return {}
        finally:
            if 'cursor' in locals():
                cursor.close()

    def select_weighted_strategies(
            self,
            market_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        시장 상황에 따라 가중치를 적용하여 전략을 선택합니다.

        Args:
            market_data: 시장 데이터 및 상황

        Returns:
            List[Dict]: 가중치가 적용된 전략 목록
        """
        self.logger.info("가중치 기반 전략 선택 중")

        # 전략 가중치 초기화
        strategy_scores = {
            strategy_id: {
                "id": strategy_id,
                "name": info["name"],
                "description": info["description"],
                "score": info["default_weight"],
                "suitable_conditions": info["suitable_conditions"]
            }
            for strategy_id, info in self.supported_strategies.items()
        }

        # 전략 우선순위 가중치 가져오기
        priority_weights = self.get_strategy_priority(market_data)

        # 기존 가중치에 우선순위 가중치 적용
        for strategy_id, strategy_info in strategy_scores.items():
            strategy_scores[strategy_id]["score"] *= priority_weights.get(strategy_id, 1.0)

        # 시장 상황에 따른 가중치 조정
        if market_data:
            primary_condition = market_data.get('primary_condition', 'unknown')
            secondary_condition = market_data.get('secondary_condition', 'unknown')
            market_phase = market_data.get('market_phase', 'unknown')
            trend_direction = market_data.get('trend', {}).get('direction', 'unknown')

            for strategy_id, strategy_info in strategy_scores.items():
                suitable_conditions = strategy_info["suitable_conditions"]

                # 주요 시장 상황과 일치하면 점수 증가
                if primary_condition in suitable_conditions:
                    strategy_scores[strategy_id]["score"] += 0.3

                # 보조 시장 상황과 일치하면 점수 증가
                if secondary_condition in suitable_conditions:
                    strategy_scores[strategy_id]["score"] += 0.2

                # 시장 국면과 관련이 있으면 점수 증가
                if market_phase in suitable_conditions:
                    strategy_scores[strategy_id]["score"] += 0.1

                # 추세 방향에 따른 조정
                if trend_direction in ['strongly_up', 'up'] and strategy_id in ['trend_following', 'breakout']:
                    strategy_scores[strategy_id]["score"] += 0.15
                elif trend_direction in ['strongly_down', 'down'] and strategy_id in ['trend_following']:
                    strategy_scores[strategy_id]["score"] += 0.15
                elif trend_direction == 'neutral' and strategy_id in ['range', 'reversal']:
                    strategy_scores[strategy_id]["score"] += 0.15

            # 추가 시장 데이터 고려
            volatility_level = market_data.get('volatility', {}).get('level', 'medium')
            trend_strength = market_data.get('trend', {}).get('strength', 0)

            # 변동성 기반 조정
            if volatility_level in ['high', 'very_high']:
                strategy_scores['breakout']["score"] += 0.1
                strategy_scores['range']["score"] -= 0.1
            elif volatility_level in ['low', 'very_low']:
                strategy_scores['range']["score"] += 0.15
                strategy_scores['breakout']["score"] -= 0.05

            # 추세 강도 기반 조정
            if trend_strength > 1.5:
                strategy_scores['trend_following']["score"] += 0.15
                strategy_scores['range']["score"] -= 0.1
            elif trend_strength < 0.5:
                strategy_scores['range']["score"] += 0.1
                strategy_scores['trend_following']["score"] -= 0.05

        # 점수 기준 내림차순 정렬
        ranked_strategies = sorted(
            strategy_scores.values(),
            key=lambda x: x["score"],
            reverse=True
        )

        return ranked_strategies

    def evaluate_strategy_historical_performance(
            self,
            strategy_id: str,
            market_condition: str
    ) -> Dict[str, Any]:
        """
        특정 시장 상황에서 전략의 과거 성능을 평가합니다.

        Args:
            strategy_id: 전략 ID
            market_condition: 시장 상황

        Returns:
            Dict: 성능 평가 결과
        """
        # 실제 구현에서는 과거 데이터에서 성능 통계를 가져와야 함
        # 여기서는 간단한 예시 결과를 반환

        # 전략별 기본 성능 프로필
        performance_profiles = {
            "trend_following": {
                "trending_up": {"win_rate": 0.75, "profit_factor": 2.1, "avg_trade": 2.5},
                "trending_down": {"win_rate": 0.68, "profit_factor": 1.8, "avg_trade": 2.2},
                "ranging": {"win_rate": 0.35, "profit_factor": 0.8, "avg_trade": -0.5}
            },
            "reversal": {
                "trending_up": {"win_rate": 0.30, "profit_factor": 0.7, "avg_trade": -0.8},
                "trending_down": {"win_rate": 0.25, "profit_factor": 0.6, "avg_trade": -1.2},
                "ranging": {"win_rate": 0.60, "profit_factor": 1.5, "avg_trade": 1.8}
            },
            "breakout": {
                "trending_up": {"win_rate": 0.65, "profit_factor": 1.9, "avg_trade": 2.3},
                "trending_down": {"win_rate": 0.60, "profit_factor": 1.7, "avg_trade": 2.0},
                "ranging": {"win_rate": 0.50, "profit_factor": 1.4, "avg_trade": 1.5}
            },
            "range": {
                "trending_up": {"win_rate": 0.30, "profit_factor": 0.7, "avg_trade": -0.7},
                "trending_down": {"win_rate": 0.35, "profit_factor": 0.8, "avg_trade": -0.6},
                "ranging": {"win_rate": 0.70, "profit_factor": 2.0, "avg_trade": 2.4}
            }
        }

        # 전략 및 시장 상황에 맞는 성능 가져오기
        strategy_profile = performance_profiles.get(strategy_id, {})
        condition_performance = strategy_profile.get(
            market_condition,
            {"win_rate": 0.50, "profit_factor": 1.0, "avg_trade": 0.0}
        )

        # 무작위성 추가 (실제 구현에서는 실제 과거 성능 데이터 사용)
        randomize = lambda x: max(0, min(x + random.uniform(-0.05, 0.05), 1.0)) if isinstance(x, float) else x

        return {
            "strategy_id": strategy_id,
            "market_condition": market_condition,
            "win_rate": randomize(condition_performance["win_rate"]),
            "profit_factor": max(0, condition_performance["profit_factor"] + random.uniform(-0.2, 0.2)),
            "avg_trade_pct": condition_performance["avg_trade"] + random.uniform(-0.3, 0.3),
            "sample_size": random.randint(30, 100)
        }

    def get_strategy_priority(self, market_condition: Dict[str, Any]) -> Dict[str, float]:
        """
        현재 시장 상황에 따라 전략 우선순위를 결정합니다.

        Args:
            market_condition: 시장 상황 데이터

        Returns:
            Dict[str, float]: 전략별 우선순위 가중치
        """
        # 기본 우선순위 가중치
        priority_weights = {
            "trend_following": 1.0,
            "reversal": 1.0,
            "breakout": 1.0,
            "range": 1.0
        }

        # 시장 상황에 따른 우선순위 조정
        primary_condition = market_condition.get('primary_condition', 'unknown')
        secondary_condition = market_condition.get('secondary_condition', 'unknown')

        # 추세 시장에서는 추세추종, 돌파 전략 우선
        if primary_condition in ['trending_up', 'trending_down']:
            priority_weights["trend_following"] = 1.4
            priority_weights["breakout"] = 1.2
            priority_weights["reversal"] = 0.8
            priority_weights["range"] = 0.6

        # 횡보 시장에서는 레인지, 반전 전략 우선
        elif primary_condition in ['ranging', 'consolidation']:
            priority_weights["range"] = 1.4
            priority_weights["reversal"] = 1.2
            priority_weights["breakout"] = 0.9
            priority_weights["trend_following"] = 0.7

        # 고변동성 시장에서는 돌파, 반전 전략 우선
        elif primary_condition in ['high_volatility', 'very_high_volatility']:
            priority_weights["breakout"] = 1.4
            priority_weights["reversal"] = 1.2
            priority_weights["trend_following"] = 0.9
            priority_weights["range"] = 0.6

        # 과매수/과매도 상태에서는 반전 전략 우선
        if secondary_condition in ['overbought', 'oversold', 'euphoria', 'capitulation']:
            priority_weights["reversal"] *= 1.3

        # 누적/배분 구간에서는 해당 전략 강화
        if secondary_condition == 'accumulation':
            priority_weights["range"] *= 1.2
            priority_weights["breakout"] *= 1.1
        elif secondary_condition == 'distribution':
            priority_weights["reversal"] *= 1.1
            priority_weights["range"] *= 1.0

        return priority_weights

    async def get_ai_strategy_recommendation(
            self,
            symbol: str
    ) -> Dict[str, Any]:
        """
        Gemini 또는 Claude API를 사용하여 전략 추천을 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: AI 전략 추천
        """
        self.logger.info(f"AI 전략 추천 요청 중: {symbol}")

        # 데이터 수집
        market_condition = self.get_market_condition(symbol)
        mtf_analysis = self.get_multi_timeframe_analysis(symbol)
        chart_patterns = self.get_chart_patterns(symbol)
        liquidation_clusters = self.get_liquidation_clusters(symbol)
        onchain_data = self.get_onchain_data()
        fear_greed_data = self.get_fear_greed_data()

        # AI 모델 선택 (무작위로 Gemini 또는 Claude 사용)
        use_gemini = random.choice([True, False])

        try:
            # 분석 데이터 요약
            data_summary = {
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "market_condition": market_condition,
                "mtf_analysis": {
                    "dominant_trend": mtf_analysis.get("dominant_trend", "unknown") if mtf_analysis else "unknown",
                    "coherence": mtf_analysis.get("timeframe_coherence", 0) if mtf_analysis else 0,
                    "recommendations": mtf_analysis.get("recommendations", {}) if mtf_analysis else {}
                },
                "chart_patterns": [
                    {
                        "timeframe": pattern.get("timeframe"),
                        "type": pattern.get("pattern_type"),
                        "confidence": pattern.get("confidence")
                    }
                    for pattern in chart_patterns[:3]  # 상위 3개만
                ],
                "liquidation_clusters": {
                    "support_clusters": [
                                            cluster for cluster in liquidation_clusters.get("clusters", [])
                                            if cluster.get("type") == "support"
                                        ][:3],
                    "resistance_clusters": [
                                               cluster for cluster in liquidation_clusters.get("clusters", [])
                                               if cluster.get("type") == "resistance"
                                           ][:3]
                },
                "onchain_sentiment": "neutral",
                "fear_greed": fear_greed_data
            }

            # 온체인 데이터에서 감정 추출
            if onchain_data and onchain_data.get("market_analysis", {}).get("market_sentiment"):
                data_summary["onchain_sentiment"] = onchain_data.get("market_analysis", {}).get("market_sentiment")

            coherence_raw = data_summary['mtf_analysis'].get('coherence', 0)

            # 안전한 float 변환 및 포매팅
            try:
                coherence_float = float(coherence_raw)
            except (ValueError, TypeError):
                coherence_float = 0.0

            # AI 프롬프트 구성
            prompt = f"""
You are an elite Bitcoin trading strategy analyst. Analyze the provided market data and recommend the optimal trading strategy for the current market conditions.

CURRENT MARKET DATA:
- Symbol: {symbol}
- Primary Market Condition: {market_condition.get('primary_condition', 'unknown') if market_condition else 'unknown'}
- Secondary Market Condition: {market_condition.get('secondary_condition', 'unknown') if market_condition else 'unknown'}
- Market Phase: {market_condition.get('market_phase', 'unknown') if market_condition else 'unknown'}
- Dominant Trend (MTF Analysis): {data_summary['mtf_analysis']['dominant_trend']}
- Timeframe Coherence: {coherence_float:.2f}
- Current Volatility Level: {market_condition.get('volatility', {}).get('level', 'unknown') if market_condition else 'unknown'}
- Trend Strength: {market_condition.get('trend', {}).get('strength', 0) if market_condition else 0}
- Fear & Greed Index: {fear_greed_data.get('fear_greed_index', 'N/A')} ({fear_greed_data.get('sentiment_category', 'N/A')})
- Onchain Sentiment: {data_summary['onchain_sentiment']}

SIGNIFICANT CHART PATTERNS:
{json.dumps(data_summary['chart_patterns'], indent=2)}

LIQUIDATION CLUSTERS:
{json.dumps(data_summary['liquidation_clusters'], indent=2)}

AVAILABLE STRATEGIES:
1. Trend Following - Follow established medium/long-term trends, suitable for trending markets
2. Reversal - Anticipate trend reversals at extreme overbought/oversold conditions
3. Breakout - Enter positions when price breaks significant levels with increased volume
4. Range - Trade between support and resistance in sideways markets

REQUIREMENTS:
1. Using HTF → LTF analysis methodology, determine the optimal primary and secondary trading strategies
2. Provide detailed reasoning for your selection, with specific reference to the provided data
3. Include entry criteria for the recommended strategies
4. Recommend optimal TP/SL levels based on significant support/resistance and liquidation clusters
5. Estimate a confidence score (0-100%) for your recommendation

Respond with a JSON object in the following format:
{{
            "primary_strategy": string,
  "secondary_strategy": string,
  "confidence": float,
  "reasoning": string,
  "entry_criteria": [string, string, ...],
  "tp_levels": [float, float, ...],
  "sl_level": float,
  "ranking": [
    {{"strategy": string, "score": float, "reasoning": string}},
    ...
  ]
}}
"""
            # API를 통한 추천 가져오기
            api_response = None
            if use_gemini:
                try:
                    # Gemini API 호출
                    model = self.gemini_client.models.generate_content(
                        model="gemini-2.0-pro-exp-02-05",
                        contents=prompt
                    )
                    self.logger.info(f"Gemini API 프롬프트:\n{prompt}")
                    api_response = model.text
                    self.logger.info("Gemini API 응답 받음")
                except Exception as e:
                    self.logger.error(f"Gemini API 오류: {e}")
            else:
                try:
                    # Claude API 호출
                    message = self.claude_client.messages.create(
                        model="claude-3-opus-20240229",
                        max_tokens=2000,
                        system="You are an elite cryptocurrency trading strategy analyst.",
                        messages=[
                            {
                                "role": "user",
                                "content": prompt
                            }
                        ]
                    )
                    api_response = message.content[0].text
                    self.logger.info("Claude API 응답 받음")
                except Exception as e:
                    self.logger.error(f"Claude API 오류: {e}")

            # API 응답이 없으면 내부 알고리즘 사용
            if not api_response:
                self.logger.warning("AI API 응답 없음, 내부 알고리즘 사용")
                ranked_strategies = self.select_weighted_strategies(market_condition)

                # 현재 가격 가져오기 추가
                current_price = self.get_current_price(symbol)

                # 기본 결과 구성
                result = {
                    "primary_strategy": ranked_strategies[0]["id"] if ranked_strategies else "trend_following",
                    "secondary_strategy": ranked_strategies[1]["id"] if len(ranked_strategies) > 1 else "breakout",
                    "confidence": 70.0,
                    "reasoning": "Based on internal algorithm analysis of market conditions and trend patterns.",
                    "entry_criteria": [
                        "Wait for candle close above key resistance" if ranked_strategies and ranked_strategies[0][
                            "id"] == "breakout" else
                        "Enter on pullbacks to moving average in trending market" if ranked_strategies and
                                                                                     ranked_strategies[0][
                                                                                         "id"] == "trend_following" else
                        "Enter near support level in ranging market"
                    ],
                    "tp_levels": [
                        current_price * 1.05 if current_price else 0,
                        current_price * 1.1 if current_price else 0
                    ],
                    "sl_level": current_price * 0.95 if current_price else 0,
                    "ranking": [
                        {
                            "strategy": strategy["id"],
                            "score": strategy["score"],
                            "reasoning": f"Suitable for {', '.join(strategy['suitable_conditions'][:2])}"
                        }
                        for strategy in ranked_strategies[:4]
                    ]
                }

                return result

            # API 응답 파싱
            try:
                # JSON 부분 추출
                json_str = api_response.strip()
                if "```json" in json_str:
                    json_str = json_str.split("```json")[1].split("```")[0].strip()
                elif "```" in json_str:
                    json_str = json_str.split("```")[1].split("```")[0].strip()

                result = json.loads(json_str)
                self.logger.info(f"AI 전략 추천 파싱 완료: {result['primary_strategy']}/{result['secondary_strategy']}")

                return result

            except Exception as e:
                self.logger.error(f"AI 응답 파싱 오류: {e}")

                # 파싱 실패 시 내부 알고리즘 사용
                ranked_strategies = self.select_weighted_strategies(market_condition)

                # 기본 결과 구성
                return {
                    "primary_strategy": ranked_strategies[0]["id"] if ranked_strategies else "trend_following",
                    "secondary_strategy": ranked_strategies[1]["id"] if len(ranked_strategies) > 1 else "breakout",
                    "confidence": 60.0,
                    "reasoning": "Based on internal algorithm after AI response parsing failure.",
                    "entry_criteria": ["Use price action confirmation before entry"],
                    "tp_levels": [],
                    "sl_level": 0,
                    "ranking": [
                        {
                            "strategy": strategy["id"],
                            "score": strategy["score"],
                            "reasoning": "Based on internal algorithm"
                        }
                        for strategy in ranked_strategies[:4]
                    ]
                }

        except Exception as e:
            self.logger.error(f"AI 전략 추천 프로세스 오류: {e}")
            return {
                "primary_strategy": "trend_following",
                "secondary_strategy": "breakout",
                "confidence": 50.0,
                "reasoning": "Fallback recommendation due to error in recommendation process.",
                "entry_criteria": ["Use extra caution due to recommendation system error"],
                "tp_levels": [],
                "sl_level": 0,
                "ranking": []
            }

    def get_current_price(self, symbol: str) -> Optional[float]:
        """
        특정 심볼의 현재 가격을 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Optional[float]: 현재 가격 또는 None
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    close 
                FROM 
                    market_ohlcv 
                WHERE 
                    symbol = %s AND timeframe = '1m'
                ORDER BY 
                    timestamp_ms DESC 
                LIMIT 1
                """,
                (symbol,)
            )

            result = cursor.fetchone()
            if result:
                return float(result[0])

            return None

        except Exception as e:
            self.logger.error(f"현재 가격 조회 중 오류 발생: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def save_strategy_recommendation(
            self,
            symbol: str,
            recommendation: Dict[str, Any]
    ) -> None:
        """
        전략 추천 결과를 저장합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            recommendation: 전략 추천 결과
        """
        timestamp_ms = int(time.time() * 1000)

        # 시장 상황 가져오기
        market_condition = self.get_market_condition(symbol)

        # Redis에 저장
        redis_key = f"strategy:recommendation:{symbol.replace('/', '_')}"
        self.redis_client.set(redis_key, json.dumps({
            "symbol": symbol,
            "timestamp_ms": timestamp_ms,
            "recommendation": recommendation,
            "market_condition": market_condition
        }))
        self.redis_client.expire(redis_key, 86400)  # 1일 후 만료

        # PostgreSQL에 저장
        cursor = self.db_conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO ai_strategy_recommendations 
                (symbol, timestamp_ms, primary_strategy, secondary_strategy, strategies_ranking,
                market_condition, confidence, entry_points, tp_sl_recommendations, ai_reasoning)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp_ms) 
                DO UPDATE SET
                    primary_strategy = EXCLUDED.primary_strategy,
                    secondary_strategy = EXCLUDED.secondary_strategy,
                    strategies_ranking = EXCLUDED.strategies_ranking,
                    market_condition = EXCLUDED.market_condition,
                    confidence = EXCLUDED.confidence,
                    entry_points = EXCLUDED.entry_points,
                    tp_sl_recommendations = EXCLUDED.tp_sl_recommendations,
                    ai_reasoning = EXCLUDED.ai_reasoning,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    symbol,
                    timestamp_ms,
                    recommendation.get('primary_strategy', 'unknown'),
                    recommendation.get('secondary_strategy', 'unknown'),
                    json.dumps(recommendation.get('ranking', [])),
                    json.dumps(market_condition) if market_condition else '{}',
                    recommendation.get('confidence', 0),
                    json.dumps(recommendation.get('entry_criteria', [])),
                    json.dumps({
                        'tp_levels': recommendation.get('tp_levels', []),
                        'sl_level': recommendation.get('sl_level', 0)
                    }),
                    recommendation.get('reasoning', '')
                )
            )

            self.db_conn.commit()
            self.logger.info(f"전략 추천 저장됨: {symbol}")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"전략 추천 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

    async def select_optimal_strategy(
            self,
            symbol: str = "BTC/USDT"
    ) -> Dict[str, Any]:
        """
        현재 시장 상황에 최적화된 전략을 선택합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 전략 선택 결과
        """
        self.logger.info(f"최적 전략 선택 시작: {symbol}")

        try:
            # 가중치 기반 전략 선택
            market_condition = self.get_market_condition(symbol)
            weighted_strategies = self.select_weighted_strategies(market_condition)

            # AI 전략 추천 가져오기
            ai_recommendation = await self.get_ai_strategy_recommendation(symbol)

            # 결과 저장
            self.save_strategy_recommendation(symbol, ai_recommendation)

            # 결과 구성
            result = {
                "symbol": symbol,
                "timestamp_ms": int(time.time() * 1000),
                "ai_recommendation": ai_recommendation,
                "weighted_strategies": weighted_strategies,
                "market_condition": market_condition
            }

            self.logger.info(f"최적 전략 선택 완료: {symbol}")
            return result

        except Exception as e:
            self.logger.error(f"최적 전략 선택 중 오류 발생: {e}")
            return {
                "symbol": symbol,
                "timestamp_ms": int(time.time() * 1000),
                "error": str(e)
            }

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")

        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            self.logger.info("Redis 연결 종료됨")


# 직접 실행 시 선택기 시작
if __name__ == "__main__":
    selector = AIStrategySelector()

    try:
        # 비동기 실행을 위한 이벤트 루프
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(selector.select_optimal_strategy("BTC/USDT"))

        print(f"===== AI 전략 추천 결과 =====")

        ai_rec = result.get('ai_recommendation', {})
        print(f"주요 전략: {ai_rec.get('primary_strategy', 'unknown')}")
        print(f"보조 전략: {ai_rec.get('secondary_strategy', 'unknown')}")
        print(f"신뢰도: {ai_rec.get('confidence', 0):.2f}%")

        print(f"\n===== 추천 근거 =====")
        print(ai_rec.get('reasoning', 'No reasoning provided'))

        print(f"\n===== 진입 기준 =====")
        for criteria in ai_rec.get('entry_criteria', []):
            print(f"- {criteria}")

        print(f"\n===== TP/SL 레벨 =====")
        print(f"TP 레벨: {ai_rec.get('tp_levels', [])}")
        print(f"SL 레벨: {ai_rec.get('sl_level', 0)}")

    except Exception as e:
        print(f"전략 선택 오류: {e}")
    finally:
        selector.close()
        if 'loop' in locals():
            loop.close()
