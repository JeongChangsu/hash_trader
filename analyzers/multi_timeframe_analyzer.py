# analyzers/multi_timeframe_analyzer.py
"""
다중 시간프레임 분석기

이 모듈은 다양한 시간프레임의 차트 분석 결과를 통합합니다.
HTF에서 LTF로의 분석 접근법을 구현하여 전체적인 시장 상황을
평가하고 시간프레임 간 정합성을 검사합니다.
"""

import logging
import json
import numpy as np
import pandas as pd
import redis
import psycopg2
import time
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from config.settings import POSTGRES_CONFIG, REDIS_CONFIG, TIMEFRAMES
from config.logging_config import configure_logging


class MultiTimeframeAnalyzer:
    """
    다중 시간프레임 분석을 수행하는 클래스
    """

    def __init__(self):
        """MultiTimeframeAnalyzer 초기화"""
        self.logger = configure_logging("multi_timeframe_analyzer")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 시간프레임 정의 (큰 것부터 작은 것 순서)
        self.timeframes = TIMEFRAMES
        self.timeframes.sort(key=self.timeframe_to_minutes, reverse=True)

        # 시간프레임별 가중치 (HTF가 더 높은 가중치)
        self.tf_weights = self.calculate_timeframe_weights()

        # 분석 캐시 (분석 결과 재사용)
        self.analysis_cache = {}

        self.initialize_db()
        self.logger.info("MultiTimeframeAnalyzer 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS multi_timeframe_analysis (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    tf_coherence FLOAT NOT NULL,
                    dominant_trend VARCHAR(20) NOT NULL,
                    htf_signals JSONB NOT NULL,
                    ltf_signals JSONB NOT NULL,
                    recommendations JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_multi_tf_lookup 
                ON multi_timeframe_analysis (symbol, timestamp_ms);
            """)
            self.db_conn.commit()
            self.logger.info("다중 시간프레임 분석 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def timeframe_to_minutes(self, tf: str) -> int:
        """
        시간프레임 문자열을 분 단위 숫자로 변환합니다.

        Args:
            tf: 시간프레임 문자열 (예: '1h', '4h', '1d')

        Returns:
            int: 시간프레임의 분 단위 값
        """
        if tf.endswith('m'):
            return int(tf[:-1])
        elif tf.endswith('h'):
            return int(tf[:-1]) * 60
        elif tf.endswith('d'):
            return int(tf[:-1]) * 60 * 24
        elif tf.endswith('w'):
            return int(tf[:-1]) * 60 * 24 * 7
        elif tf.endswith('M'):
            return int(tf[:-1]) * 60 * 24 * 30
        else:
            return 0

    def calculate_timeframe_weights(self) -> Dict[str, float]:
        """
        각 시간프레임에 대한 가중치를 계산합니다.

        Returns:
            Dict[str, float]: 시간프레임별 가중치
        """
        weights = {}

        # 로그 스케일로 가중치 부여 (큰 시간프레임이 더 높은 가중치)
        total_weight = 0

        for i, tf in enumerate(self.timeframes):
            # 지수 감소 가중치 (HTF가 더 높은 가중치)
            weight = np.exp(-0.5 * i)
            weights[tf] = weight
            total_weight += weight

        # 정규화 (가중치 합이 1이 되도록)
        for tf in weights:
            weights[tf] /= total_weight

        return weights

    def get_chart_analysis(
            self,
            symbol: str,
            timeframe: str
    ) -> Optional[Dict[str, Any]]:
        """
        특정 시간프레임에 대한 차트 분석 결과를 가져옵니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')
            timeframe: 시간프레임 (예: '1h')

        Returns:
            Optional[Dict]: 차트 분석 결과 또는 None
        """
        cache_key = f"{symbol}_{timeframe}"

        # 캐시에 있으면 캐시된 결과 반환
        if cache_key in self.analysis_cache:
            return self.analysis_cache[cache_key]

        # Redis에서 분석 결과 가져오기
        redis_key = f"analysis:chart:{symbol.replace('/', '_')}:{timeframe}"
        cached_data = self.redis_client.get(redis_key)

        if cached_data:
            try:
                analysis = json.loads(cached_data)
                self.analysis_cache[cache_key] = analysis
                return analysis
            except json.JSONDecodeError:
                self.logger.error(f"차트 분석 JSON 파싱 오류: {symbol} ({timeframe})")

        # Redis에 없으면 DB에서 직접 가져오기
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    pattern_data
                FROM 
                    chart_patterns
                WHERE 
                    symbol = %s AND timeframe = %s
                ORDER BY 
                    created_at DESC, confidence DESC
                LIMIT 10
                """,
                (symbol, timeframe)
            )

            patterns = []
            for row in cursor.fetchall():
                pattern_data = json.loads(row[0])
                patterns.append(pattern_data)

            cursor.execute(
                """
                SELECT 
                    level_type, price_level, strength, touch_count
                FROM 
                    support_resistance_levels
                WHERE 
                    symbol = %s AND timeframe = %s
                ORDER BY 
                    created_at DESC, strength DESC
                """,
                (symbol, timeframe)
            )

            support = []
            resistance = []

            for row in cursor.fetchall():
                level_type, price_level, strength, touch_count = row
                level_data = {
                    'price': float(price_level),
                    'strength': float(strength),
                    'touch_count': int(touch_count)
                }

                if level_type == 'support':
                    support.append(level_data)
                else:
                    resistance.append(level_data)

            # 결과 구성
            analysis = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': int(time.time() * 1000),
                'patterns': patterns,
                'support_resistance': {
                    'support': support,
                    'resistance': resistance
                }
            }

            # 캐시에 저장
            self.analysis_cache[cache_key] = analysis
            return analysis

        except Exception as e:
            self.logger.error(f"DB에서 차트 분석 가져오기 실패: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def extract_trend_signals(
            self,
            analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        분석 결과에서 추세 신호를 추출합니다.

        Args:
            analysis: 차트 분석 결과

        Returns:
            Dict: 추세 신호
        """
        signals = {
            'trend': analysis.get('trend', 'neutral'),
            'patterns': [],
            'support': [],
            'resistance': [],
            'breakout_signals': [],
            'reversal_signals': []
        }

        # 패턴 분석
        patterns = analysis.get('patterns', [])

        for pattern in patterns:
            pattern_type = pattern.get('pattern_type', '')
            confidence = pattern.get('confidence', 0)

            # 신뢰도 50% 이상만 고려
            if confidence < 50:
                continue

            # 패턴 유형에 따른 신호 분류
            if pattern_type == 'double_bottom' or pattern_type == 'inverse_head_and_shoulders':
                signals['reversal_signals'].append({
                    'type': pattern_type,
                    'direction': 'bullish',
                    'confidence': confidence,
                    'target_price': pattern.get('target_price'),
                    'stop_loss': pattern.get('stop_loss_price')
                })

            elif pattern_type == 'double_top' or pattern_type == 'head_and_shoulders':
                signals['reversal_signals'].append({
                    'type': pattern_type,
                    'direction': 'bearish',
                    'confidence': confidence,
                    'target_price': pattern.get('target_price'),
                    'stop_loss': pattern.get('stop_loss_price')
                })

            elif 'triangle' in pattern_type:
                signals['breakout_signals'].append({
                    'type': pattern_type,
                    'direction': 'bullish' if pattern_type == 'ascending_triangle' else
                    ('bearish' if pattern_type == 'descending_triangle' else 'neutral'),
                    'confidence': confidence,
                    'target_price': pattern.get('target_price'),
                    'stop_loss': pattern.get('stop_loss_price')
                })

            elif pattern_type == 'bullish_flag':
                signals['breakout_signals'].append({
                    'type': pattern_type,
                    'direction': 'bullish',
                    'confidence': confidence,
                    'target_price': pattern.get('target_price'),
                    'stop_loss': pattern.get('stop_loss_price')
                })

            elif pattern_type == 'bearish_flag':
                signals['breakout_signals'].append({
                    'type': pattern_type,
                    'direction': 'bearish',
                    'confidence': confidence,
                    'target_price': pattern.get('target_price'),
                    'stop_loss': pattern.get('stop_loss_price')
                })

            # 모든 패턴 저장 (간략화)
            signals['patterns'].append({
                'type': pattern_type,
                'confidence': confidence
            })

        # 지지/저항 레벨
        for support in analysis.get('support_resistance', {}).get('support', []):
            signals['support'].append({
                'price': support.get('price'),
                'strength': support.get('strength')
            })

        for resistance in analysis.get('support_resistance', {}).get('resistance', []):
            signals['resistance'].append({
                'price': resistance.get('price'),
                'strength': resistance.get('strength')
            })

        return signals

    def analyze_htf_to_ltf(
            self,
            symbol: str
    ) -> Dict[str, Any]:
        """
        HTF에서 LTF로의 분석을 수행합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 통합된 분석 결과
        """
        self.logger.info(f"HTF → LTF 분석 시작: {symbol}")

        results = {
            'symbol': symbol,
            'timestamp_ms': int(time.time() * 1000),
            'htf_signals': {},
            'ltf_signals': {},
            'timeframe_coherence': 0.0,
            'dominant_trend': 'neutral',
            'confidence': 0.0,
            'recommendations': {}
        }

        try:
            # 각 시간프레임에 대한 분석 결과 가져오기
            tf_analysis = {}
            tf_signals = {}

            for tf in self.timeframes:
                analysis = self.get_chart_analysis(symbol, tf)

                if not analysis:
                    self.logger.warning(f"{tf} 분석 결과 없음, 건너뜀")
                    continue

                tf_analysis[tf] = analysis
                tf_signals[tf] = self.extract_trend_signals(analysis)

            if not tf_analysis:
                raise Exception("어떤 시간프레임에 대한 분석 결과도 없음")

            # 시간프레임을 HTF와 LTF로 분리
            htf = []
            ltf = []

            # 상위 절반은 HTF, 하위 절반은 LTF로 간주
            midpoint = len(self.timeframes) // 2
            htf_timeframes = self.timeframes[:midpoint]
            ltf_timeframes = self.timeframes[midpoint:]

            # HTF 신호 통합
            htf_signals = {}
            for tf in htf_timeframes:
                if tf in tf_signals:
                    htf_signals[tf] = tf_signals[tf]

            # LTF 신호 통합
            ltf_signals = {}
            for tf in ltf_timeframes:
                if tf in tf_signals:
                    ltf_signals[tf] = tf_signals[tf]

            # 시간프레임 간 추세 정합성 검사
            trend_counts = {'uptrend': 0, 'downtrend': 0, 'neutral': 0}
            weighted_trend_scores = {'uptrend': 0.0, 'downtrend': 0.0, 'neutral': 0.0}

            total_weight = 0.0
            for tf, signals in tf_signals.items():
                if tf in self.tf_weights:
                    trend = signals.get('trend', 'neutral')
                    weight = self.tf_weights[tf]

                    trend_counts[trend] += 1
                    weighted_trend_scores[trend] += weight
                    total_weight += weight

            # 지배적 추세 결정
            dominant_trend = max(weighted_trend_scores.items(), key=lambda x: x[1])[0]

            # 추세 정합성 계산 (지배적 추세와 일치하는 시간프레임 비율)
            if total_weight > 0:
                trend_coherence = weighted_trend_scores[dominant_trend] / total_weight
            else:
                trend_coherence = 0.0

            # 추세 신뢰도 계산
            trend_confidence = trend_coherence * 100

            # 패턴 기반 신호 추출
            bullish_signals = []
            bearish_signals = []

            # HTF에서 중요한 패턴 찾기
            for tf, signals in htf_signals.items():
                # 반전 신호
                for signal in signals.get('reversal_signals', []):
                    if signal['direction'] == 'bullish':
                        bullish_signals.append({
                            'timeframe': tf,
                            'type': signal['type'],
                            'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                            'target_price': signal.get('target_price'),
                            'stop_loss': signal.get('stop_loss')
                        })
                    elif signal['direction'] == 'bearish':
                        bearish_signals.append({
                            'timeframe': tf,
                            'type': signal['type'],
                            'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                            'target_price': signal.get('target_price'),
                            'stop_loss': signal.get('stop_loss')
                        })

                # 돌파 신호
                for signal in signals.get('breakout_signals', []):
                    if signal['direction'] == 'bullish':
                        bullish_signals.append({
                            'timeframe': tf,
                            'type': signal['type'],
                            'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                            'target_price': signal.get('target_price'),
                            'stop_loss': signal.get('stop_loss')
                        })
                    elif signal['direction'] == 'bearish':
                        bearish_signals.append({
                            'timeframe': tf,
                            'type': signal['type'],
                            'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                            'target_price': signal.get('target_price'),
                            'stop_loss': signal.get('stop_loss')
                        })

            # LTF에서 진입 시점 찾기
            entry_signals = []

            for tf, signals in ltf_signals.items():
                # HTF 추세 방향과 일치하는 LTF 패턴 찾기
                if dominant_trend == 'uptrend':
                    for signal in signals.get('reversal_signals', []):
                        if signal['direction'] == 'bullish':
                            entry_signals.append({
                                'timeframe': tf,
                                'type': signal['type'],
                                'direction': 'bullish',
                                'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                                'target_price': signal.get('target_price'),
                                'stop_loss': signal.get('stop_loss')
                            })

                    for signal in signals.get('breakout_signals', []):
                        if signal['direction'] == 'bullish':
                            entry_signals.append({
                                'timeframe': tf,
                                'type': signal['type'],
                                'direction': 'bullish',
                                'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                                'target_price': signal.get('target_price'),
                                'stop_loss': signal.get('stop_loss')
                            })

                elif dominant_trend == 'downtrend':
                    for signal in signals.get('reversal_signals', []):
                        if signal['direction'] == 'bearish':
                            entry_signals.append({
                                'timeframe': tf,
                                'type': signal['type'],
                                'direction': 'bearish',
                                'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                                'target_price': signal.get('target_price'),
                                'stop_loss': signal.get('stop_loss')
                            })

                    for signal in signals.get('breakout_signals', []):
                        if signal['direction'] == 'bearish':
                            entry_signals.append({
                                'timeframe': tf,
                                'type': signal['type'],
                                'direction': 'bearish',
                                'confidence': signal['confidence'] * self.tf_weights.get(tf, 0.5),
                                'target_price': signal.get('target_price'),
                                'stop_loss': signal.get('stop_loss')
                            })

            # 지지/저항 레벨 통합
            support_levels = []
            resistance_levels = []

            # 모든 시간프레임의 지지/저항 레벨 통합 (가중치 적용)
            for tf, signals in tf_signals.items():
                weight = self.tf_weights.get(tf, 0.5)

                for support in signals.get('support', []):
                    support_levels.append({
                        'timeframe': tf,
                        'price': support.get('price'),
                        'strength': support.get('strength') * weight
                    })

                for resistance in signals.get('resistance', []):
                    resistance_levels.append({
                        'timeframe': tf,
                        'price': resistance.get('price'),
                        'strength': resistance.get('strength') * weight
                    })

            # 중복된 레벨 병합 (비슷한 가격 = 0.5% 이내)
            merged_support = self.merge_price_levels(support_levels, 0.005)
            merged_resistance = self.merge_price_levels(resistance_levels, 0.005)

            # 강도순으로 정렬
            merged_support.sort(key=lambda x: x['strength'], reverse=True)
            merged_resistance.sort(key=lambda x: x['strength'], reverse=True)

            # 키 레벨 선택 (상위 5개)
            key_support = merged_support[:5]
            key_resistance = merged_resistance[:5]

            # 최종 추천 생성
            recommendations = {
                'action': 'neutral',
                'entry_signals': sorted(entry_signals, key=lambda x: x['confidence'], reverse=True),
                'key_support': key_support,
                'key_resistance': key_resistance,
                'dominant_trend': dominant_trend,
                'trend_confidence': trend_confidence
            }

            # 지배적인 추세와 높은 정합성이 있는 경우 추천 생성
            if trend_confidence > 70:
                if dominant_trend == 'uptrend' and entry_signals:
                    # 상위 진입 신호 찾기
                    top_bullish_entry = max(
                        [s for s in entry_signals if s['direction'] == 'bullish'],
                        key=lambda x: x['confidence'],
                        default=None
                    )

                    if top_bullish_entry:
                        recommendations['action'] = 'buy'
                        recommendations['primary_entry'] = top_bullish_entry

                        # 손절가 설정 (가장 가까운 지지 레벨 아래)
                        if key_support:
                            lower_supports = [s for s in key_support if
                                              s['price'] < top_bullish_entry.get('target_price', float('inf'))]
                            if lower_supports:
                                closest_support = max(lower_supports, key=lambda x: x['price'])
                                recommendations['stop_loss_price'] = closest_support['price'] * 0.99
                            else:
                                recommendations['stop_loss_price'] = top_bullish_entry.get('stop_loss')
                        else:
                            recommendations['stop_loss_price'] = top_bullish_entry.get('stop_loss')

                        # 목표가 설정 (가장 가까운 저항 레벨)
                        if key_resistance:
                            higher_resistance = [r for r in key_resistance if
                                                 r['price'] > top_bullish_entry.get('target_price', 0)]
                            if higher_resistance:
                                closest_resistance = min(higher_resistance, key=lambda x: x['price'])
                                recommendations['target_price'] = closest_resistance['price']
                            else:
                                recommendations['target_price'] = top_bullish_entry.get('target_price')
                        else:
                            recommendations['target_price'] = top_bullish_entry.get('target_price')

                elif dominant_trend == 'downtrend' and entry_signals:
                    # 상위 진입 신호 찾기
                    top_bearish_entry = max(
                        [s for s in entry_signals if s['direction'] == 'bearish'],
                        key=lambda x: x['confidence'],
                        default=None
                    )

                    if top_bearish_entry:
                        recommendations['action'] = 'sell'
                        recommendations['primary_entry'] = top_bearish_entry

                        # 손절가 설정 (가장 가까운 저항 레벨 위)
                        if key_resistance:
                            higher_resistance = [r for r in key_resistance if
                                                 r['price'] > top_bearish_entry.get('target_price', 0)]
                            if higher_resistance:
                                closest_resistance = min(higher_resistance, key=lambda x: x['price'])
                                recommendations['stop_loss_price'] = closest_resistance['price'] * 1.01
                            else:
                                recommendations['stop_loss_price'] = top_bearish_entry.get('stop_loss')
                        else:
                            recommendations['stop_loss_price'] = top_bearish_entry.get('stop_loss')

                        # 목표가 설정 (가장 가까운 지지 레벨)
                        if key_support:
                            lower_supports = [s for s in key_support if
                                              s['price'] < top_bearish_entry.get('target_price', float('inf'))]
                            if lower_supports:
                                closest_support = max(lower_supports, key=lambda x: x['price'])
                                recommendations['target_price'] = closest_support['price']
                            else:
                                recommendations['target_price'] = top_bearish_entry.get('target_price')
                        else:
                            recommendations['target_price'] = top_bearish_entry.get('target_price')

            # 결과 구성
            results['htf_signals'] = htf_signals
            results['ltf_signals'] = ltf_signals
            results['timeframe_coherence'] = trend_coherence
            results['dominant_trend'] = dominant_trend
            results['confidence'] = trend_confidence
            results['recommendations'] = recommendations

            # 결과 저장
            self.save_analysis_results(results)

            self.logger.info(
                f"HTF → LTF 분석 완료: {symbol}, 추세: {dominant_trend}, 신뢰도: {trend_confidence:.2f}%, 추천: {recommendations['action']}")

        except Exception as e:
            self.logger.error(f"HTF → LTF 분석 중 오류 발생: {e}")
            results['error'] = str(e)

        return results

    def merge_price_levels(
            self,
            levels: List[Dict[str, Any]],
            threshold: float = 0.005
    ) -> List[Dict[str, Any]]:
        """
        비슷한 가격 레벨을 병합합니다.

        Args:
            levels: 가격 레벨 목록
            threshold: 동일 가격으로 간주할 임계값 (비율)

        Returns:
            List[Dict]: 병합된 가격 레벨 목록
        """
        if not levels:
            return []

        # 가격 기준으로 정렬
        sorted_levels = sorted(levels, key=lambda x: x.get('price', 0))

        # 병합된 레벨을 저장할 리스트
        merged = []

        # 현재 그룹
        current_group = [sorted_levels[0]]
        current_price = sorted_levels[0].get('price', 0)

        # 나머지 레벨 처리
        for level in sorted_levels[1:]:
            price = level.get('price', 0)

            # 현재 그룹의 가격과 비교
            if abs(price - current_price) / current_price <= threshold:
                # 비슷한 가격이면 현재 그룹에 추가
                current_group.append(level)
            else:
                # 새 그룹 시작 전에 현재 그룹 병합하여 추가
                merged.append(self.combine_levels(current_group))

                # 새 그룹 시작
                current_group = [level]
                current_price = price

        # 마지막 그룹 추가
        if current_group:
            merged.append(self.combine_levels(current_group))

        return merged

    def combine_levels(
            self,
            levels: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        여러 가격 레벨을 하나로 결합합니다.

        Args:
            levels: 결합할 가격 레벨 목록

        Returns:
            Dict: 결합된 가격 레벨
        """
        if not levels:
            return {}

        # 합산할 값들
        total_strength = sum(level.get('strength', 0) for level in levels)
        total_price = sum(level.get('price', 0) * level.get('strength', 1) for level in levels)

        # 가중 평균 가격 계산
        if total_strength > 0:
            avg_price = total_price / total_strength
        else:
            avg_price = levels[0].get('price', 0)

        # 포함된 시간프레임 목록
        timeframes = list(set(level.get('timeframe', 'unknown') for level in levels))

        return {
            'price': avg_price,
            'strength': total_strength,
            'timeframes': timeframes,
            'source_count': len(levels)
        }

    def check_timeframe_coherence(
            self,
            symbol: str
    ) -> Dict[str, Any]:
        """
        시간프레임 간 정합성을 검사합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 정합성 분석 결과
        """
        # HTF → LTF 분석과 동일한 결과를 제공하기 위해 해당 함수 호출
        return self.analyze_htf_to_ltf(symbol)

    def calculate_multi_timeframe_momentum(
            self,
            symbol: str
    ) -> Dict[str, Any]:
        """
        다중 시간프레임 모멘텀을 계산합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 모멘텀 분석 결과
        """
        self.logger.info(f"다중 시간프레임 모멘텀 계산: {symbol}")

        result = {
            'symbol': symbol,
            'timestamp_ms': int(time.time() * 1000),
            'momentum_score': 0,
            'timeframe_momentum': {}
        }

        try:
            # 각 시간프레임별 모멘텀 지표 계산
            for tf in self.timeframes:
                # 차트 분석 결과 가져오기
                analysis = self.get_chart_analysis(symbol, tf)

                if not analysis:
                    continue

                # 추세 및 패턴 신호 추출
                signals = self.extract_trend_signals(analysis)

                # 모멘텀 점수 초기화
                momentum = 0

                # 추세 기반 모멘텀
                if signals.get('trend') == 'uptrend':
                    momentum += 1
                elif signals.get('trend') == 'downtrend':
                    momentum -= 1

                # 반전 신호 기반 모멘텀
                for signal in signals.get('reversal_signals', []):
                    if signal['direction'] == 'bullish':
                        momentum += signal['confidence'] / 100
                    elif signal['direction'] == 'bearish':
                        momentum -= signal['confidence'] / 100

                # 돌파 신호 기반 모멘텀
                for signal in signals.get('breakout_signals', []):
                    if signal['direction'] == 'bullish':
                        momentum += signal['confidence'] / 100
                    elif signal['direction'] == 'bearish':
                        momentum -= signal['confidence'] / 100

                # 시간프레임별 모멘텀 저장
                result['timeframe_momentum'][tf] = {
                    'momentum': momentum,
                    'weight': self.tf_weights.get(tf, 0.5)
                }

            # 가중 모멘텀 점수 계산
            total_momentum = 0
            total_weight = 0

            for tf, momentum_data in result['timeframe_momentum'].items():
                momentum = momentum_data['momentum']
                weight = momentum_data['weight']

                total_momentum += momentum * weight
                total_weight += weight

            # 정규화된 모멘텀 점수 계산 (-100% ~ +100%)
            if total_weight > 0:
                result['momentum_score'] = (total_momentum / total_weight) * 100

            # 모멘텀 강도 해석
            if result['momentum_score'] > 75:
                result['momentum_strength'] = 'very_strong_bullish'
            elif result['momentum_score'] > 50:
                result['momentum_strength'] = 'strong_bullish'
            elif result['momentum_score'] > 25:
                result['momentum_strength'] = 'bullish'
            elif result['momentum_score'] > 10:
                result['momentum_strength'] = 'slightly_bullish'
            elif result['momentum_score'] > -10:
                result['momentum_strength'] = 'neutral'
            elif result['momentum_score'] > -25:
                result['momentum_strength'] = 'slightly_bearish'
            elif result['momentum_score'] > -50:
                result['momentum_strength'] = 'bearish'
            elif result['momentum_score'] > -75:
                result['momentum_strength'] = 'strong_bearish'
            else:
                result['momentum_strength'] = 'very_strong_bearish'

            self.logger.info(
                f"모멘텀 계산 완료: {symbol}, 점수: {result['momentum_score']:.2f}, 강도: {result['momentum_strength']}")

        except Exception as e:
            self.logger.error(f"모멘텀 계산 중 오류 발생: {e}")
            result['error'] = str(e)

        return result

    def save_analysis_results(
            self,
            results: Dict[str, Any]
    ) -> None:
        """
        분석 결과를 저장합니다.

        Args:
            results: 분석 결과
        """
        symbol = results.get('symbol')
        timestamp_ms = results.get('timestamp_ms')

        if not symbol or not timestamp_ms:
            self.logger.error("저장할 분석 결과에 필수 필드가 없음")
            return

        # Redis에 저장
        redis_key = f"analysis:multi_tf:{symbol.replace('/', '_')}"
        self.redis_client.set(redis_key, json.dumps(results))
        self.redis_client.expire(redis_key, 86400)  # 1일 후 만료

        # PostgreSQL에 저장
        cursor = self.db_conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO multi_timeframe_analysis 
                (symbol, timestamp_ms, tf_coherence, dominant_trend, htf_signals, ltf_signals, recommendations)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp_ms) 
                DO UPDATE SET
                    tf_coherence = EXCLUDED.tf_coherence,
                    dominant_trend = EXCLUDED.dominant_trend,
                    htf_signals = EXCLUDED.htf_signals,
                    ltf_signals = EXCLUDED.ltf_signals,
                    recommendations = EXCLUDED.recommendations,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    symbol,
                    timestamp_ms,
                    float(results.get('timeframe_coherence', 0)),  # 명시적으로 float로 변환
                    results.get('dominant_trend', 'neutral'),
                    json.dumps(results.get('htf_signals', {})),
                    json.dumps(results.get('ltf_signals', {})),
                    json.dumps(results.get('recommendations', {}))
                )
            )

            self.db_conn.commit()
            self.logger.info(f"분석 결과 저장됨: {symbol}")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"분석 결과 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

    def run_analysis(
            self,
            symbol: str = "BTC/USDT"
    ) -> Dict[str, Any]:
        """
        다중 시간프레임 분석을 실행합니다.

        Args:
            symbol: 심볼 (예: 'BTC/USDT')

        Returns:
            Dict: 분석 결과
        """
        self.logger.info(f"다중 시간프레임 분석 시작: {symbol}")

        try:
            # HTF → LTF 분석 수행
            htf_ltf_results = self.analyze_htf_to_ltf(symbol)

            # 모멘텀 계산
            momentum_results = self.calculate_multi_timeframe_momentum(symbol)

            # 결과 통합
            combined_results = htf_ltf_results.copy()
            combined_results['momentum'] = momentum_results.get('momentum_score', 0)
            combined_results['momentum_strength'] = momentum_results.get('momentum_strength', 'neutral')

            # 추천에 모멘텀 정보 추가
            if 'recommendations' in combined_results:
                combined_results['recommendations']['momentum'] = momentum_results.get('momentum_score', 0)
                combined_results['recommendations']['momentum_strength'] = momentum_results.get('momentum_strength',
                                                                                                'neutral')

            self.logger.info(f"다중 시간프레임 분석 완료: {symbol}")
            return combined_results

        except Exception as e:
            self.logger.error(f"다중 시간프레임 분석 중 오류 발생: {e}")
            return {
                'symbol': symbol,
                'timestamp_ms': int(time.time() * 1000),
                'error': str(e)
            }

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
    analyzer = MultiTimeframeAnalyzer()

    try:
        results = analyzer.run_analysis("BTC/USDT")

        print(f"===== 다중 시간프레임 분석 결과 =====")
        print(f"심볼: {results['symbol']}")
        print(f"지배적 추세: {results['dominant_trend']} (신뢰도: {results['confidence']:.2f}%)")
        print(f"모멘텀: {results.get('momentum', 0):.2f} ({results.get('momentum_strength', 'neutral')})")
        print(f"시간프레임 정합성: {results['timeframe_coherence']:.2f}")

        recommendations = results.get('recommendations', {})
        print(f"\n===== 추천 행동 =====")
        print(f"행동: {recommendations.get('action', 'neutral')}")

        if recommendations.get('action') != 'neutral':
            print(f"목표가: {recommendations.get('target_price', 0):.2f}")
            print(f"손절가: {recommendations.get('stop_loss_price', 0):.2f}")

        print(f"\n===== HTF 신호 =====")
        for tf, signals in results.get('htf_signals', {}).items():
            print(f"{tf}: {signals.get('trend', 'neutral')}")

        print(f"\n===== LTF 신호 =====")
        for tf, signals in results.get('ltf_signals', {}).items():
            print(f"{tf}: {signals.get('trend', 'neutral')}")

    except Exception as e:
        print(f"분석 오류: {e}")
    finally:
        analyzer.close()
