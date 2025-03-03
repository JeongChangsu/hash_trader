# scripts/reset_database.py
"""
HashTrader 데이터베이스 초기화 간소화 스크립트

이 스크립트는 슈퍼유저 권한 없이도 테이블 데이터를 삭제하고
새로운 테이블을 생성합니다.
"""

import os
import sys
import redis
import asyncio
import logging
import argparse
import psycopg2

from datetime import datetime

# 프로젝트 루트 디렉토리를 시스템 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import POSTGRES_CONFIG, REDIS_CONFIG
from data_collectors.backfill_ohlcv import backfill_ohlcv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_reset")


def connect_db():
    """PostgreSQL 데이터베이스에 연결합니다."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        logger.info("PostgreSQL 데이터베이스에 연결됨")
        return conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 실패: {e}")
        raise


def reset_redis(keep_ohlcv: bool = False):
    """
    Redis 데이터베이스를 초기화합니다.

    Args:
        keep_ohlcv: OHLCV 데이터를 보존할지 여부
    """
    try:
        redis_client = redis.Redis(**REDIS_CONFIG)

        if keep_ohlcv:
            # OHLCV 데이터 키 패턴
            ohlcv_patterns = ["data:market:ohlcv:*", "stream:market:ohlcv:*"]

            # OHLCV 키 백업
            ohlcv_keys = set()
            for pattern in ohlcv_patterns:
                ohlcv_keys.update(redis_client.keys(pattern))

            # OHLCV 키가 아닌 모든 키 삭제
            all_keys = redis_client.keys("*")
            for key in all_keys:
                if key not in ohlcv_keys:
                    redis_client.delete(key)

            logger.info(f"OHLCV 데이터({len(ohlcv_keys)}개 키)를 제외한 모든 Redis 키가 삭제되었습니다")
        else:
            # 모든 키 삭제
            redis_client.flushdb()
            logger.info("모든 Redis 키가 삭제되었습니다")

        redis_client.close()
    except Exception as e:
        logger.error(f"Redis 초기화 중 오류 발생: {e}")


def check_table_exists(conn, table_name):
    """테이블이 존재하는지 확인합니다."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def reset_tables(conn, keep_ohlcv: bool = False):
    """
    테이블 데이터를 초기화하고 필요시 테이블을 생성합니다.

    Args:
        conn: PostgreSQL 연결 객체
        keep_ohlcv: OHLCV 테이블을 보존할지 여부
    """
    cursor = conn.cursor()

    try:
        # 데이터만 삭제 (테이블 구조는 유지)
        tables_to_truncate = []

        if not keep_ohlcv:
            tables_to_truncate.append("market_ohlcv")

        tables_to_truncate.extend([
            "market_orderbook",
            "volume_profile"
        ])

        # 종속성 있는 테이블들은 순서에 주의
        dependent_tables = [
            ("liquidation_clusters", "liquidation_heatmap"),
            ("onchain_metrics", "onchain_data"),
            ("fear_greed_index", None)
        ]

        # 독립적인 테이블 먼저 초기화
        for table in tables_to_truncate:
            if check_table_exists(conn, table):
                cursor.execute(f"TRUNCATE TABLE {table};")
                logger.info(f"{table} 테이블 데이터가 삭제되었습니다")

        # 종속성 있는 테이블 초기화
        for child_table, parent_table in dependent_tables:
            # 자식 테이블이 있으면 먼저 삭제
            if check_table_exists(conn, child_table):
                cursor.execute(f"TRUNCATE TABLE {child_table} CASCADE;")
                logger.info(f"{child_table} 테이블 데이터가 삭제되었습니다")

            # 부모 테이블이 있고 None이 아니면 삭제
            if parent_table and check_table_exists(conn, parent_table):
                cursor.execute(f"TRUNCATE TABLE {parent_table} CASCADE;")
                logger.info(f"{parent_table} 테이블 데이터가 삭제되었습니다")

        conn.commit()
        logger.info("테이블 데이터 삭제 완료")

        # 새로운 스키마로 테이블 생성
        logger.info("필요한 테이블 생성 중...")

        # 1. 청산 히트맵 테이블 (존재하지 않으면 생성)
        if not check_table_exists(conn, "liquidation_heatmap"):
            cursor.execute("""
                CREATE TABLE liquidation_heatmap (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (collection_id)
                );
            """)
            logger.info("liquidation_heatmap 테이블이 생성되었습니다")

        if not check_table_exists(conn, "liquidation_clusters"):
            cursor.execute("""
                CREATE TABLE liquidation_clusters (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,
                    price_low NUMERIC NOT NULL,
                    price_high NUMERIC NOT NULL,
                    cluster_type VARCHAR(20) NOT NULL,
                    intensity VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (collection_id) REFERENCES liquidation_heatmap(collection_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_liquidation_clusters_collection_id 
                ON liquidation_clusters(collection_id);

                CREATE INDEX IF NOT EXISTS idx_liquidation_clusters_price_range 
                ON liquidation_clusters(price_low, price_high);
            """)
            logger.info("liquidation_clusters 테이블이 생성되었습니다")

        # 2. 공포&탐욕 지수 테이블
        if not check_table_exists(conn, "fear_greed_index"):
            cursor.execute("""
                CREATE TABLE fear_greed_index (
                    id SERIAL,
                    timestamp_ms BIGINT NOT NULL,
                    fear_greed_index INTEGER,
                    sentiment_category VARCHAR(50),
                    change_rate FLOAT,
                    date_value DATE PRIMARY KEY,
                    signal VARCHAR(50)
                );

                CREATE INDEX IF NOT EXISTS idx_fear_greed_date 
                ON fear_greed_index (date_value);
            """)
            logger.info("fear_greed_index 테이블이 생성되었습니다")

        # 3. 온체인 데이터 테이블
        if not check_table_exists(conn, "onchain_data"):
            cursor.execute("""
                CREATE TABLE onchain_data (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (collection_id)
                );
            """)
            logger.info("onchain_data 테이블이 생성되었습니다")

        if not check_table_exists(conn, "onchain_metrics"):
            cursor.execute("""
                CREATE TABLE onchain_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp_ms BIGINT NOT NULL,
                    collection_id VARCHAR(15) NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value NUMERIC NOT NULL,
                    change_24h NUMERIC,
                    signal VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (collection_id) REFERENCES onchain_data(collection_id) ON DELETE CASCADE,
                    UNIQUE (collection_id, metric_name)
                );

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_collection_id 
                ON onchain_metrics(collection_id);

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_timestamp 
                ON onchain_metrics(timestamp_ms);

                CREATE INDEX IF NOT EXISTS idx_onchain_metrics_name 
                ON onchain_metrics(metric_name);
            """)
            logger.info("onchain_metrics 테이블이 생성되었습니다")

        # 4. 거래량 프로필 테이블
        if not check_table_exists(conn, "volume_profile"):
            cursor.execute("""
                CREATE TABLE volume_profile (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    start_time_ms BIGINT NOT NULL,
                    end_time_ms BIGINT NOT NULL,
                    price_levels JSONB NOT NULL,
                    poc NUMERIC,
                    value_area_high NUMERIC,
                    value_area_low NUMERIC,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timeframe, start_time_ms, end_time_ms)
                );
            """)
            logger.info("volume_profile 테이블이 생성되었습니다")

        # 5. 오더북 데이터 테이블
        if not check_table_exists(conn, "market_orderbook"):
            cursor.execute("""
                CREATE TABLE market_orderbook (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    bids JSONB NOT NULL,
                    asks JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_market_orderbook_lookup 
                ON market_orderbook (exchange, symbol, timestamp_ms);
            """)
            logger.info("market_orderbook 테이블이 생성되었습니다")

        # 6. OHLCV 데이터 테이블 (keep_ohlcv가 False이고 테이블이 없는 경우에만)
        if not keep_ohlcv and not check_table_exists(conn, "market_ohlcv"):
            cursor.execute("""
                CREATE TABLE market_ohlcv (
                    id SERIAL PRIMARY KEY,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    open NUMERIC NOT NULL,
                    high NUMERIC NOT NULL,
                    low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (exchange, symbol, timeframe, timestamp_ms)
                );

                CREATE INDEX IF NOT EXISTS idx_market_ohlcv_lookup 
                ON market_ohlcv (exchange, symbol, timeframe, timestamp_ms);
            """)
            logger.info("market_ohlcv 테이블이 생성되었습니다")

        conn.commit()
        logger.info("모든 테이블 생성/초기화 완료")

    except Exception as e:
        conn.rollback()
        logger.error(f"테이블 초기화/생성 중 오류 발생: {e}")
    finally:
        cursor.close()


def modify_existing_tables(conn):
    """
    기존 테이블 구조를 수정합니다 (필요한 경우).
    새로운 컬럼 추가 또는 테이블 구조 변경을 처리합니다.
    """
    cursor = conn.cursor()
    try:
        # 1. liquidation_heatmap에 collection_id 추가
        if check_table_exists(conn, "liquidation_heatmap"):
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'liquidation_heatmap' AND column_name = 'collection_id';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE liquidation_heatmap ADD COLUMN collection_id VARCHAR(15);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_liquidation_heatmap_collection_id 
                    ON liquidation_heatmap(collection_id);
                """)
                logger.info("liquidation_heatmap 테이블에 collection_id 컬럼 추가됨")

        # 2. liquidation_clusters에 collection_id 추가
        if check_table_exists(conn, "liquidation_clusters"):
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'liquidation_clusters' AND column_name = 'collection_id';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE liquidation_clusters ADD COLUMN collection_id VARCHAR(15);
                    CREATE INDEX IF NOT EXISTS idx_liquidation_clusters_collection_id 
                    ON liquidation_clusters(collection_id);
                """)
                logger.info("liquidation_clusters 테이블에 collection_id 컬럼 추가됨")

        # 3. onchain_data에 collection_id 추가
        if check_table_exists(conn, "onchain_data"):
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'onchain_data' AND column_name = 'collection_id';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE onchain_data ADD COLUMN collection_id VARCHAR(15);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_onchain_data_collection_id 
                    ON onchain_data(collection_id);
                """)
                logger.info("onchain_data 테이블에 collection_id 컬럼 추가됨")

        # 4. onchain_metrics에 collection_id 추가
        if check_table_exists(conn, "onchain_metrics"):
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'onchain_metrics' AND column_name = 'collection_id';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE onchain_metrics ADD COLUMN collection_id VARCHAR(15);
                    CREATE INDEX IF NOT EXISTS idx_onchain_metrics_collection_id 
                    ON onchain_metrics(collection_id);
                """)
                logger.info("onchain_metrics 테이블에 collection_id 컬럼 추가됨")

        # 5. fear_greed_index 테이블 PK 변경
        if check_table_exists(conn, "fear_greed_index"):
            cursor.execute("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'fear_greed_index' AND constraint_type = 'PRIMARY KEY';
            """)
            constraint = cursor.fetchone()

            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'fear_greed_index' AND column_name = 'date_value';
            """)
            has_date_value = cursor.fetchone()

            # date_value 컬럼이 없으면 추가
            if not has_date_value:
                cursor.execute("""
                    ALTER TABLE fear_greed_index ADD COLUMN date_value DATE;
                    UPDATE fear_greed_index 
                    SET date_value = (timestamp_ms / 86400000)::int::date
                    WHERE date_value IS NULL;
                """)
                logger.info("fear_greed_index 테이블에 date_value 컬럼 추가됨")

            # 제약 조건 확인 및 수정 (적절한 권한이 있는 경우)
            try:
                if constraint:
                    cursor.execute(f"""
                        ALTER TABLE fear_greed_index DROP CONSTRAINT {constraint[0]};
                        ALTER TABLE fear_greed_index ADD PRIMARY KEY (date_value);
                    """)
                    logger.info("fear_greed_index 테이블 PK가 date_value로 변경됨")
            except Exception as e:
                logger.warning(f"fear_greed_index PK 변경 시도 중 오류: {e}")
                logger.warning("권한 문제로 인해 PK 변경을 건너뜁니다")

        conn.commit()
        logger.info("기존 테이블 구조 수정 완료")

    except Exception as e:
        conn.rollback()
        logger.error(f"테이블 구조 수정 중 오류 발생: {e}")
    finally:
        cursor.close()


async def run_backfill(days: int = 365):
    """
    OHLCV 과거 데이터를 수집합니다.

    Args:
        days: 백필할 일수
    """
    try:
        logger.info(f"OHLCV 과거 데이터 {days}일치 백필 시작")
        await backfill_ohlcv()
        logger.info("OHLCV 데이터 백필 완료")
    except Exception as e:
        logger.error(f"백필 중 오류 발생: {e}")


async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="HashTrader 데이터베이스 초기화 도구")

    parser.add_argument("--keep-ohlcv", action="store_true",
                        help="OHLCV 데이터를 보존합니다")
    parser.add_argument("--backfill", action="store_true",
                        help="OHLCV 과거 데이터를 수집합니다")
    parser.add_argument("--days", type=int, default=365,
                        help="백필할 과거 데이터 일수 (기본값: 365)")
    parser.add_argument("--redis-only", action="store_true",
                        help="Redis 데이터베이스만 초기화합니다")
    parser.add_argument("--postgres-only", action="store_true",
                        help="PostgreSQL 데이터베이스만 초기화합니다")

    args = parser.parse_args()

    # 실행 시작 로그
    start_time = datetime.now()
    logger.info(f"데이터베이스 초기화 시작: {start_time}")

    try:
        # Redis 초기화
        if not args.postgres_only:
            reset_redis(args.keep_ohlcv)

        # PostgreSQL 초기화 및 수정
        if not args.redis_only:
            conn = connect_db()
            reset_tables(conn, args.keep_ohlcv)
            modify_existing_tables(conn)
            conn.close()

        # OHLCV 백필
        if args.backfill:
            await run_backfill(args.days)

        # 종료 로그
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"데이터베이스 초기화 완료: {end_time} (소요 시간: {duration:.1f}초)")

    except Exception as e:
        logger.error(f"초기화 프로세스 중 오류 발생: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("키보드 인터럽트로 종료")
