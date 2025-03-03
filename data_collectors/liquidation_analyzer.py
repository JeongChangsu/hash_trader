# data_collectors/liquidation_analyzer.py
"""
청산 히트맵 분석기

이 모듈은 비트코인 청산 히트맵을 수집하고 분석합니다.
Coinglass에서 히트맵 이미지를 다운로드하고 Gemini API를 사용하여
지지/저항 레벨과 청산 클러스터를 파악합니다.
분석 결과를 기반으로 TP/SL 설정에 대한 추천을 제공합니다.

한국 시간(KST) 기준으로 데이터를 수집하고 저장합니다.
"""

import os
import re
import time
import json
import pytz
import asyncio
import logging
import psycopg2

import pandas as pd
import redis.asyncio as redis

from PIL import Image
from google import genai
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from typing import Dict, List, Any, Optional, Tuple
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config.settings import (
    GEMINI_API_KEY,
    HEATMAP_IMAGES_DIR,
    REDIS_CONFIG,
    POSTGRES_CONFIG
)
from config.logging_config import configure_logging

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')


class LiquidationAnalyzer:
    """
    비트코인 청산 히트맵 분석 클래스
    """

    def __init__(self):
        """LiquidationAnalyzer 초기화"""
        self.logger = configure_logging("liquidation_analyzer")

        # Redis 연결 정보
        self.redis_params = {k: v for k, v in REDIS_CONFIG.items() if k != 'decode_responses'}

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 설정 및 경로
        self.heatmap_url = "https://www.coinglass.com/pro/futures/LiquidationHeatMap"
        self.redis_stream = "data:liquidation_heatmap:latest"
        self.heatmaps_dir = HEATMAP_IMAGES_DIR
        os.makedirs(self.heatmaps_dir, exist_ok=True)

        # Gemini API 설정
        self.gemini_client = genai.Client(api_key=GEMINI_API_KEY)

        self.initialize_db()
        self.logger.info("LiquidationAnalyzer 초기화됨")

    def initialize_db(self) -> None:
        """PostgreSQL 테이블을 초기화합니다."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS liquidation_heatmap (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,  -- 날짜_시간 형식 (YYYYMMDD_HH)
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (collection_id)
                );

                CREATE TABLE IF NOT EXISTS liquidation_clusters (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,  -- 날짜_시간 형식 (YYYYMMDD_HH)
                    price_low NUMERIC NOT NULL,
                    price_high NUMERIC NOT NULL,
                    cluster_type VARCHAR(20) NOT NULL,
                    intensity VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (collection_id) REFERENCES liquidation_heatmap(collection_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_liquidation_heatmap_collection_id 
                ON liquidation_heatmap(collection_id);

                CREATE INDEX IF NOT EXISTS idx_liquidation_clusters_collection_id 
                ON liquidation_clusters(collection_id);

                CREATE INDEX IF NOT EXISTS idx_liquidation_clusters_price_range 
                ON liquidation_clusters(price_low, price_high);
            """)
            self.db_conn.commit()
            self.logger.info("청산 히트맵 테이블 초기화됨")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"테이블 초기화 중 오류 발생: {e}")
        finally:
            cursor.close()

    def download_heatmap_image(self, max_attempts: int = 3) -> Optional[str]:
        """
        Coinglass에서 청산 히트맵 이미지를 다운로드합니다.

        Args:
            max_attempts: 최대 시도 횟수

        Returns:
            Optional[str]: 다운로드된 이미지의 경로, 실패 시 None
        """
        for attempt in range(1, max_attempts + 1):
            try:
                options = webdriver.ChromeOptions()
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")

                prefs = {"download.default_directory": self.heatmaps_dir}
                options.add_experimental_option("prefs", prefs)

                now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                self.logger.info(f"청산 히트맵 다운로드 시도 중 ({attempt}/{max_attempts}) (KST: {now_kst})")

                driver = webdriver.Chrome(options=options)
                driver.get(self.heatmap_url)

                # 페이지 로딩 대기
                wait = WebDriverWait(driver, 30)

                # 카메라 버튼 찾기 및 클릭
                camera_buttons = wait.until(
                    EC.presence_of_all_elements_located((By.XPATH,
                                                         '//div[contains(@class, "MuiStack-root")]//button[contains(@class, "MuiButton-variantSoft")]'))
                )

                if not camera_buttons or len(camera_buttons) < 2:
                    raise Exception("카메라 버튼을 찾을 수 없습니다")

                camera_buttons[1].click()

                # 다운로드 완료 대기
                time.sleep(10)

                # 파일 찾기
                files = [f for f in os.listdir(self.heatmaps_dir) if 'Binance BTC_USDT Liquidation Heatmap' in f]

                if not files:
                    raise Exception("다운로드된 히트맵 파일을 찾을 수 없습니다")

                # 가장 최근 파일 선택
                latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(self.heatmaps_dir, x)))
                image_path = os.path.join(self.heatmaps_dir, latest_file)

                self.logger.info(f"청산 히트맵 다운로드 성공: {image_path}")
                return image_path

            except Exception as e:
                self.logger.error(f"히트맵 다운로드 실패 ({attempt}/{max_attempts}): {e}")

                if attempt == max_attempts:
                    self.logger.error("최대 시도 횟수 초과, 다운로드 실패")
                    return None

                # 재시도 전 대기
                time.sleep(10)
            finally:
                if 'driver' in locals():
                    driver.quit()

        return None

    def analyze_heatmap_with_gemini(self, image_path: str) -> str:
        """
        Gemini API를 사용하여 청산 히트맵을 분석합니다.

        Args:
            image_path: 분석할 이미지 경로

        Returns:
            str: 분석 결과 텍스트
        """
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"Gemini API로 청산 히트맵 분석 중: {image_path} (KST: {now_kst})")

        prompt = '''Analyze the attached Bitcoin liquidation heatmap image for the past 24 hours.
Identify clearly defined liquidation clusters with precise price ranges (e.g., '41850–42000'), cluster type ('support' or 'resistance'), and intensity ('low', 'medium', 'high').

Additionally, provide actionable insights explicitly suggesting optimal stop-loss (SL) placement below high-intensity support clusters and cautious take-profit (TP) placement near or below medium/high-intensity resistance clusters.

Provide the analysis strictly as JSON:
{"clusters": [{"price_range": "xxxx–xxxx", "type": "support/resistance", "intensity": "low/medium/high"}], "actionable_insight": "Explicit TP and SL suggestions based on identified clusters."}

If no notable clusters are identified, explicitly return: {"clusters": [], "actionable_insight": "No notable clusters found."}'''

        try:
            image = Image.open(image_path)

            response = self.gemini_client.models.generate_content(
                model="gemini-2.0-pro-exp-02-05",
                contents=[prompt, image]
            )

            self.logger.info("Gemini API 분석 완료")
            return response.text

        except Exception as e:
            self.logger.error(f"Gemini API 분석 실패: {e}")
            return json.dumps({
                "clusters": [],
                "actionable_insight": f"분석 오류: {str(e)}"
            })

    def parse_analysis_result(self, analysis_text: str) -> Dict[str, Any]:
        """
        Gemini API 분석 결과를 파싱합니다.

        Args:
            analysis_text: 분석 결과 텍스트

        Returns:
            Dict: 파싱된 분석 결과
        """
        try:
            # JSON 코드 블록이 있는 경우 정리
            cleaned_text = re.sub(r'```json|```', '', analysis_text, flags=re.MULTILINE).strip()

            analysis_data = json.loads(cleaned_text)

            # 클러스터 처리
            clusters = []
            for cluster in analysis_data.get("clusters", []):
                price_range = cluster.get("price_range", "")
                if "–" in price_range or "-" in price_range:
                    # 하이픈 통일
                    price_range = price_range.replace("–", "-")

                    # 가격 범위 분리
                    price_parts = price_range.split("-")
                    if len(price_parts) == 2:
                        try:
                            price_low = float(price_parts[0].replace(",", ""))
                            price_high = float(price_parts[1].replace(",", ""))

                            clusters.append({
                                "price_range": price_range,
                                "price_low": price_low,
                                "price_high": price_high,
                                "type": cluster.get("type", "unknown"),
                                "intensity": cluster.get("intensity", "medium")
                            })
                        except ValueError:
                            self.logger.warning(f"가격 범위 파싱 실패: {price_range}")

            # 결과 구성
            result = {
                "timestamp_ms": int(time.time() * 1000),
                "clusters": clusters,
                "actionable_insight": analysis_data.get("actionable_insight", "No actionable insights provided.")
            }

            return result

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 파싱 실패: {e}")
            return {
                "timestamp_ms": int(time.time() * 1000),
                "clusters": [],
                "actionable_insight": f"파싱 오류: JSON 디코딩 실패. {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"분석 결과 파싱 중 오류 발생: {e}")
            return {
                "timestamp_ms": int(time.time() * 1000),
                "clusters": [],
                "actionable_insight": f"파싱 오류: {str(e)}"
            }

    def extract_sl_tp_recommendations(self, data: Dict[str, Any], current_price: Optional[float] = None) -> Dict[
        str, Any]:
        """
        분석 결과에서 명시적인 SL/TP 추천을 추출합니다.

        Args:
            data: 분석 결과 데이터
            current_price: 현재 비트코인 가격

        Returns:
            Dict: SL/TP 추천
        """
        # 기본 결과
        result = {
            "sl_levels": [],
            "tp_levels": [],
            "key_support_levels": [],
            "key_resistance_levels": []
        }

        # 클러스터에서 지지/저항 레벨 분류
        for cluster in data.get("clusters", []):
            cluster_type = cluster.get("type", "").lower()
            intensity = cluster.get("intensity", "").lower()
            price_low = cluster.get("price_low")
            price_high = cluster.get("price_high")

            if not price_low or not price_high:
                continue

            # 지지 레벨
            if cluster_type == "support":
                level = {
                    "price_low": price_low,
                    "price_high": price_high,
                    "intensity": intensity
                }

                result["key_support_levels"].append(level)

                # 강한 지지 레벨 아래에 SL 배치
                if intensity in ["high", "medium"]:
                    sl_price = price_low * 0.995  # 지지 레벨 0.5% 아래
                    result["sl_levels"].append({
                        "price": sl_price,
                        "reference_level": level,
                        "confidence": "high" if intensity == "high" else "medium"
                    })

            # 저항 레벨
            elif cluster_type == "resistance":
                level = {
                    "price_low": price_low,
                    "price_high": price_high,
                    "intensity": intensity
                }

                result["key_resistance_levels"].append(level)

                # 강한 저항 레벨 아래에 TP 배치
                if intensity in ["high", "medium"]:
                    tp_price = price_low * 0.99  # 저항 레벨 1% 아래
                    result["tp_levels"].append({
                        "price": tp_price,
                        "reference_level": level,
                        "confidence": "high" if intensity == "high" else "medium"
                    })

        # 각 레벨 목록 정렬
        if result["sl_levels"]:
            result["sl_levels"] = sorted(result["sl_levels"], key=lambda x: x["price"])

        if result["tp_levels"]:
            result["tp_levels"] = sorted(result["tp_levels"], key=lambda x: x["price"])

        result["key_support_levels"] = sorted(result["key_support_levels"], key=lambda x: x["price_low"])
        result["key_resistance_levels"] = sorted(result["key_resistance_levels"], key=lambda x: x["price_low"])

        return result

    def generate_collection_id(self) -> str:
        """
        수집 ID를 생성합니다. 날짜_시간 형식 (YYYYMMDD_HH)으로,
        시간은 8시, 16시, 0시 중 가장 가까운 시간대로 고정됩니다.

        Returns:
            str: 수집 ID
        """
        # 현재 한국 시간
        now = datetime.now(KST)

        # 시간대 결정 (8시, 16시, 0시 중 가장 가까운 시간)
        hour = now.hour

        if 0 <= hour < 4:
            target_hour = 0
        elif 4 <= hour < 12:
            target_hour = 8
        elif 12 <= hour < 20:
            target_hour = 16
        else:  # 20 <= hour < 24
            target_hour = 0
            # 0시의 경우 다음 날 0시를 의미
            if hour >= 20:
                now = now + timedelta(days=1)

        # YYYYMMDD_HH 형식으로 ID 생성
        collection_id = now.strftime('%Y%m%d') + f'_{target_hour:02d}'

        return collection_id

    async def save_results(self, data: Dict[str, Any]) -> None:
        """
        분석 결과를 Redis 및 PostgreSQL에 저장합니다.

        Args:
            data: 저장할 분석 결과
        """
        # 한국 시간 기반 collection_id 생성
        collection_id = self.generate_collection_id()

        # KST 시간 추가
        now = datetime.now(KST)
        data["kst_time"] = now.strftime("%Y-%m-%d %H:%M:%S")
        data["kst_date"] = now.strftime("%Y-%m-%d")
        data["collection_id"] = collection_id

        # Redis 연결
        redis_client = await redis.Redis(**self.redis_params)

        try:
            # Redis 스트림에 저장
            await redis_client.xadd(
                self.redis_stream,
                {'data': json.dumps(data, default=str)},
                maxlen=1000,
                approximate=True
            )

            # 최신 데이터로 설정
            await redis_client.set(
                "data:liquidation_heatmap:latest",
                json.dumps(data, default=str)
            )

            self.logger.info(f"분석 결과가 Redis에 저장됨 (Collection ID: {collection_id})")

        except Exception as e:
            self.logger.error(f"Redis 저장 중 오류 발생: {e}")

        finally:
            await redis_client.close()

        # PostgreSQL에 저장
        cursor = self.db_conn.cursor()
        try:
            # 메인 히트맵 데이터
            cursor.execute(
                """
                INSERT INTO liquidation_heatmap (timestamp_ms, collection_id, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (collection_id) DO UPDATE SET
                    data = EXCLUDED.data,
                    timestamp_ms = EXCLUDED.timestamp_ms,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id;
                """,
                (data['timestamp_ms'], collection_id, json.dumps(data))
            )
            self.db_conn.commit()

            # 기존 클러스터 삭제 (같은 collection_id에 해당하는)
            cursor.execute(
                """
                DELETE FROM liquidation_clusters
                WHERE collection_id = %s
                """,
                (collection_id,)
            )
            self.db_conn.commit()

            # 클러스터 데이터
            for cluster in data.get('clusters', []):
                cursor.execute(
                    """
                    INSERT INTO liquidation_clusters 
                    (timestamp_ms, collection_id, price_low, price_high, cluster_type, intensity)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        data['timestamp_ms'],
                        collection_id,
                        cluster.get('price_low'),
                        cluster.get('price_high'),
                        cluster.get('type'),
                        cluster.get('intensity')
                    )
                )

            self.db_conn.commit()
            self.logger.info(f"분석 결과가 PostgreSQL에 저장됨 (Collection ID: {collection_id})")

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"PostgreSQL 저장 중 오류 발생: {e}")

        finally:
            cursor.close()

    def get_current_price(self) -> Optional[float]:
        """
        현재 비트코인 가격을 가져옵니다.

        Returns:
            Optional[float]: 현재 비트코인 가격, 오류 시 None
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT close 
                FROM market_ohlcv 
                WHERE symbol = 'BTC/USDT' AND timeframe = '1m'
                ORDER BY timestamp_ms DESC 
                LIMIT 1
                """
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

    def get_historical_clusters(self, days: int = 7) -> pd.DataFrame:
        """
        과거 청산 클러스터 데이터를 가져옵니다.

        Args:
            days: 가져올 이전 일수

        Returns:
            pd.DataFrame: 과거 클러스터 데이터
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    lc.timestamp_ms, 
                    lc.collection_id,
                    lc.price_low, 
                    lc.price_high, 
                    lc.cluster_type, 
                    lc.intensity,
                    lh.created_at
                FROM 
                    liquidation_clusters lc
                JOIN 
                    liquidation_heatmap lh ON lc.collection_id = lh.collection_id
                WHERE 
                    lh.created_at >= NOW() - INTERVAL '%s DAY'
                ORDER BY 
                    lh.created_at DESC, lc.price_low ASC
                """,
                (days,)
            )

            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()

            df = pd.DataFrame(data, columns=columns)
            if not df.empty:
                df['created_at'] = pd.to_datetime(df['created_at'])

            return df

        except Exception as e:
            self.logger.error(f"과거 클러스터 데이터 조회 중 오류 발생: {e}")
            return pd.DataFrame()
        finally:
            if 'cursor' in locals():
                cursor.close()

    def identify_persistent_clusters(self, df: pd.DataFrame, threshold: int = 3) -> List[Dict[str, Any]]:
        """
        지속적으로 나타나는 중요 클러스터를 식별합니다.

        Args:
            df: 과거 클러스터 데이터프레임
            threshold: 중요 클러스터로 간주할 최소 출현 횟수

        Returns:
            List[Dict]: 중요 클러스터 목록
        """
        if df.empty:
            return []

        # 가격 구간 생성 (1000 단위로)
        df['price_bin'] = ((df['price_low'] + df['price_high']) / 2 // 1000) * 1000

        # 각 구간과 타입별 출현 횟수 계산
        cluster_counts = df.groupby(['price_bin', 'cluster_type']).size().reset_index(name='frequency')

        # 임계값 이상 출현한 클러스터 필터링
        important_clusters = cluster_counts[cluster_counts['frequency'] >= threshold]

        # 결과 구성
        result = []
        for _, row in important_clusters.iterrows():
            # 해당 클러스터에 속하는 모든 레코드
            cluster_data = df[
                (df['price_bin'] == row['price_bin']) &
                (df['cluster_type'] == row['cluster_type'])
                ]

            # 강도 분포 계산
            intensity_counts = cluster_data['intensity'].value_counts()
            max_intensity = intensity_counts.idxmax() if not intensity_counts.empty else "medium"

            result.append({
                "price_bin": int(row['price_bin']),
                "type": row['cluster_type'],
                "frequency": int(row['frequency']),
                "dominant_intensity": max_intensity,
                "importance": "very_high" if row['frequency'] >= threshold * 2 else "high"
            })

        return sorted(result, key=lambda x: x['price_bin'])

    async def run(self, fetch_current_price: bool = True) -> Dict[str, Any]:
        """
        청산 히트맵 분석 프로세스를 실행합니다.

        Args:
            fetch_current_price: 현재 가격 조회 여부

        Returns:
            Dict: 분석 결과
        """
        # 한국 시간으로 현재 시간 기록
        now_kst = datetime.now(KST)

        result = {
            "success": False,
            "timestamp_ms": int(time.time() * 1000),
            "kst_time": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
            "kst_date": now_kst.strftime("%Y-%m-%d"),
            "collection_id": self.generate_collection_id()
        }

        try:
            self.logger.info(f"청산 히트맵 분석 시작 (KST: {now_kst.strftime('%Y-%m-%d %H:%M:%S')})")

            # 히트맵 이미지 다운로드
            image_path = self.download_heatmap_image()
            if not image_path:
                raise Exception("청산 히트맵 이미지 다운로드 실패")

            # Gemini API로 분석
            analysis_text = self.analyze_heatmap_with_gemini(image_path)
            analysis_result = self.parse_analysis_result(analysis_text)

            # 한국 시간 정보 추가
            analysis_result["kst_time"] = now_kst.strftime("%Y-%m-%d %H:%M:%S")
            analysis_result["kst_date"] = now_kst.strftime("%Y-%m-%d")
            analysis_result["collection_id"] = result["collection_id"]

            # 현재 가격 조회
            current_price = None
            if fetch_current_price:
                current_price = self.get_current_price()
                if current_price:
                    analysis_result["current_price"] = current_price

            # TP/SL 추천 추출
            recommendations = self.extract_sl_tp_recommendations(analysis_result, current_price)
            analysis_result["recommendations"] = recommendations

            # 과거 데이터에서 중요 클러스터 식별
            historical_df = self.get_historical_clusters()
            persistent_clusters = self.identify_persistent_clusters(historical_df)
            analysis_result["persistent_clusters"] = persistent_clusters

            # 결과 저장
            await self.save_results(analysis_result)

            # 이미지 정리
            if os.path.exists(image_path):
                os.remove(image_path)
                self.logger.info(f"히트맵 이미지 삭제됨: {image_path}")

            result.update({
                "success": True,
                "data": analysis_result
            })

            self.logger.info(f"청산 히트맵 분석 완료 (Collection ID: {result['collection_id']})")

        except Exception as e:
            self.logger.error(f"청산 히트맵 분석 중 오류 발생: {e}")
            result["error"] = str(e)

        return result

    def close(self) -> None:
        """자원을 정리합니다."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            self.logger.info("PostgreSQL 연결 종료됨")


# 직접 실행 시 분석기 시작
if __name__ == "__main__":
    analyzer = LiquidationAnalyzer()

    try:
        # 비동기 실행을 위한 이벤트 루프
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(analyzer.run())

        if result["success"]:
            print(f"분석 완료. Collection ID: {result['collection_id']}")
            print(f"클러스터 수: {len(result['data'].get('clusters', []))}")
            print(f"실행 가능한 통찰: {result['data'].get('actionable_insight')}")
        else:
            print(f"분석 실패: {result.get('error', '알 수 없는 오류')}")

    except Exception as e:
        print(f"실행 중 오류 발생: {e}")
    finally:
        analyzer.close()
        if 'loop' in locals():
            loop.close()
