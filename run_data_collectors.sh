#!/bin/bash
# HashTrader 데이터 수집기 실행 스크립트 (한국 시간 기준)

# 스크립트 경로 설정
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR" || exit 1

# 로그 디렉토리 설정
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

# 한국 시간 기준 타임스탬프 생성
TIMESTAMP=$(TZ='Asia/Seoul' date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/collector_$TIMESTAMP.log"

# 현재 한국 시간 출력
KST_TIME=$(TZ='Asia/Seoul' date +"%Y-%m-%d %H:%M:%S")

# 함수: 도움말 출력
show_help() {
    echo "HashTrader 데이터 수집기 실행 스크립트 (한국 시간 기준)"
    echo ""
    echo "사용법:"
    echo "  $0 [옵션]"
    echo ""
    echo "옵션:"
    echo "  --reset             데이터베이스를 초기화합니다 (reset_database.py 사용)"
    echo "  --keep-ohlcv        OHLCV 데이터를 보존하고 나머지만 초기화합니다"
    echo "  --backfill          과거 OHLCV 데이터를 수집합니다"
    echo "  --days N            백필할 일수를 설정합니다 (기본값: 365)"
    echo "  --redis-only        Redis 데이터베이스만 초기화합니다"
    echo "  --postgres-only     PostgreSQL 데이터베이스만 초기화합니다"
    echo "  --scheduler-only    데이터베이스 초기화 없이 스케줄러만 실행합니다"
    echo "  --run-once          모든 수집기를 한 번만 실행합니다 (스케줄러 없이)"
    echo "  --market-only       시장 데이터 수집기만 한 번 실행합니다"
    echo "  --fear-greed-only   공포&탐욕 지수 수집기만 한 번 실행합니다"
    echo "  --liquidation-only  청산 히트맵 분석기만 한 번 실행합니다"
    echo "  --onchain-only      온체인 데이터 수집기만 한 번 실행합니다"
    echo "  --volume-only       거래량 프로필 분석기만 한 번 실행합니다"
    echo "  --help              도움말을 표시합니다"
    echo ""
    echo "예시:"
    echo "  $0 --reset --backfill            데이터베이스 초기화 후 백필 및 스케줄러 실행"
    echo "  $0 --keep-ohlcv --scheduler-only OHLCV 데이터 유지한 채 스케줄러만 실행"
    echo "  $0 --run-once                    모든 수집기를 한 번만 실행 (테스트용)"
    echo "  $0 --market-only                 시장 데이터 수집기만 한 번 실행"
    echo ""
}

# 기본 설정
RESET=false
KEEP_OHLCV=false
BACKFILL=false
DAYS=365
REDIS_ONLY=false
POSTGRES_ONLY=false
SCHEDULER_ONLY=false
RUN_ONCE=false
MARKET_ONLY=false
FEAR_GREED_ONLY=false
LIQUIDATION_ONLY=false
ONCHAIN_ONLY=false
VOLUME_ONLY=false

# 인자 파싱
while [[ $# -gt 0 ]]; do
    case "$1" in
        --reset)
            RESET=true
            shift
            ;;
        --keep-ohlcv)
            KEEP_OHLCV=true
            shift
            ;;
        --backfill)
            BACKFILL=true
            shift
            ;;
        --days)
            DAYS="$2"
            shift 2
            ;;
        --redis-only)
            REDIS_ONLY=true
            shift
            ;;
        --postgres-only)
            POSTGRES_ONLY=true
            shift
            ;;
        --scheduler-only)
            SCHEDULER_ONLY=true
            shift
            ;;
        --run-once)
            RUN_ONCE=true
            shift
            ;;
        --market-only)
            MARKET_ONLY=true
            shift
            ;;
        --fear-greed-only)
            FEAR_GREED_ONLY=true
            shift
            ;;
        --liquidation-only)
            LIQUIDATION_ONLY=true
            shift
            ;;
        --onchain-only)
            ONCHAIN_ONLY=true
            shift
            ;;
        --volume-only)
            VOLUME_ONLY=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "알 수 없는 옵션: $1"
            show_help
            exit 1
            ;;
    esac
done

# 로그 시작
echo "=== HashTrader 데이터 수집기 시작 (KST: $KST_TIME) ===" | tee -a "$LOG_FILE"
echo "로그파일: $LOG_FILE" | tee -a "$LOG_FILE"

# 가상환경 활성화 (있는 경우)
if [ -d "venv" ]; then
    echo "가상환경 활성화 중..." | tee -a "$LOG_FILE"
    source venv/bin/activate
fi

# 초기화 및 백필 실행
if [ "$RESET" = true ] || [ "$BACKFILL" = true ]; then
    echo "데이터베이스 관리 작업 시작..." | tee -a "$LOG_FILE"

    # 초기화 명령 구성 (reset_database.py 사용)
    RESET_CMD="python scripts/reset_database.py"

    if [ "$KEEP_OHLCV" = true ]; then
        RESET_CMD="$RESET_CMD --keep-ohlcv"
    fi

    if [ "$BACKFILL" = true ]; then
        RESET_CMD="$RESET_CMD --backfill --days $DAYS"
    fi

    if [ "$REDIS_ONLY" = true ]; then
        RESET_CMD="$RESET_CMD --redis-only"
    fi

    if [ "$POSTGRES_ONLY" = true ]; then
        RESET_CMD="$RESET_CMD --postgres-only"
    fi

    echo "실행 명령: $RESET_CMD" | tee -a "$LOG_FILE"
    eval "$RESET_CMD" 2>&1 | tee -a "$LOG_FILE"

    # 명령 실행 결과 확인
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "데이터베이스 초기화/백필 중 오류가 발생했습니다. 로그를 확인하세요." | tee -a "$LOG_FILE"
        exit 1
    fi

    echo "데이터베이스 관리 작업 완료" | tee -a "$LOG_FILE"
fi

# 단일 수집기 실행 함수
run_specific_collector() {
    local collector_type="$1"
    local collector_name="$2"

    echo "$collector_name 수집기만 실행 중..." | tee -a "$LOG_FILE"

    case "$collector_type" in
        "market")
            python -c "
import asyncio
from data_collectors.market_data_collector import MarketDataCollector

async def run_market_collector():
    collector = MarketDataCollector()
    await collector.connect()
    await collector.initialize_db_tables()
    await collector.run_collection_cycle()

    # 리소스 정리
    if hasattr(collector, 'redis_client'):
        await collector.redis_client.close()
    if hasattr(collector, 'exchange'):
        await collector.exchange.close()

    print('시장 데이터 수집 완료')

asyncio.run(run_market_collector())
" 2>&1 | tee -a "$LOG_FILE"
            ;;

        "volume")
            python -c "
import asyncio
from data_collectors.market_data_collector import MarketDataCollector

async def run_volume_profile():
    collector = MarketDataCollector()
    await collector.connect()

    # 거래량 프로필 계산
    await collector.calculate_volume_profile('BTC/USDT', '1d', 30)
    await collector.calculate_volume_profile('BTC/USDT', '4h', 120)

    # 리소스 정리
    if hasattr(collector, 'redis_client'):
        await collector.redis_client.close()
    if hasattr(collector, 'exchange'):
        await collector.exchange.close()

    print('거래량 프로필 계산 완료')

asyncio.run(run_volume_profile())
" 2>&1 | tee -a "$LOG_FILE"
            ;;

        "fear_greed")
            python -c "
from data_collectors.fear_greed_collector import FearGreedCollector

collector = FearGreedCollector()
try:
    result = collector.run()
    print(f'공포&탐욕 지수 수집 완료: {result[\"data\"][\"fear_greed_index\"]}')
finally:
    collector.close()
" 2>&1 | tee -a "$LOG_FILE"
            ;;

        "liquidation")
            python -c "
import asyncio
from data_collectors.liquidation_analyzer import LiquidationAnalyzer

async def run_liquidation_analyzer():
    analyzer = LiquidationAnalyzer()
    try:
        result = await analyzer.run()
        if result['success']:
            print(f'청산 히트맵 분석 완료: {len(result[\"data\"].get(\"clusters\", []))} 클러스터 발견')
        else:
            print(f'청산 히트맵 분석 실패: {result.get(\"error\", \"알 수 없는 오류\")}')
    finally:
        analyzer.close()

asyncio.run(run_liquidation_analyzer())
" 2>&1 | tee -a "$LOG_FILE"
            ;;

        "onchain")
            python -c "
import asyncio
from data_collectors.onchain_data_collector import OnChainDataCollector

async def run_onchain_collector():
    collector = OnChainDataCollector()
    try:
        result = await collector.run()
        if result['success']:
            print(f'온체인 데이터 수집 완료: {result[\"market_analysis\"][\"market_sentiment\"]}')
        else:
            print(f'온체인 데이터 수집 실패: {result.get(\"error\", \"알 수 없는 오류\")}')
    finally:
        collector.close()

asyncio.run(run_onchain_collector())
" 2>&1 | tee -a "$LOG_FILE"
            ;;
    esac
}

# 개별 수집기 실행
if [ "$MARKET_ONLY" = true ]; then
    run_specific_collector "market" "시장 데이터"
    exit 0
fi

if [ "$VOLUME_ONLY" = true ]; then
    run_specific_collector "volume" "거래량 프로필"
    exit 0
fi

if [ "$FEAR_GREED_ONLY" = true ]; then
    run_specific_collector "fear_greed" "공포&탐욕 지수"
    exit 0
fi

if [ "$LIQUIDATION_ONLY" = true ]; then
    run_specific_collector "liquidation" "청산 히트맵"
    exit 0
fi

if [ "$ONCHAIN_ONLY" = true ]; then
    run_specific_collector "onchain" "온체인 데이터"
    exit 0
fi

# 모든 수집기 한 번만 실행
if [ "$RUN_ONCE" = true ]; then
    echo "모든 데이터 수집기 한 번만 실행 중..." | tee -a "$LOG_FILE"

    python -c "
import asyncio
from data_collectors.market_data_collector import MarketDataCollector
from data_collectors.fear_greed_collector import FearGreedCollector
from data_collectors.liquidation_analyzer import LiquidationAnalyzer
from data_collectors.onchain_data_collector import OnChainDataCollector

async def run_all_once():
    # 시장 데이터 수집
    print('시장 데이터 수집 중...')
    collector = MarketDataCollector()
    await collector.connect()
    await collector.initialize_db_tables()
    await collector.run_collection_cycle()

    # 거래량 프로필 계산
    print('거래량 프로필 계산 중...')
    await collector.calculate_volume_profile('BTC/USDT', '1d', 30)
    await collector.calculate_volume_profile('BTC/USDT', '4h', 120)

    # 리소스 정리
    if hasattr(collector, 'redis_client'):
        await collector.redis_client.close()
    if hasattr(collector, 'exchange'):
        await collector.exchange.close()

    # 공포&탐욕 지수 수집
    print('공포&탐욕 지수 수집 중...')
    fg_collector = FearGreedCollector()
    fg_collector.run()
    fg_collector.close()

    # 청산 히트맵 분석
    print('청산 히트맵 분석 중...')
    liq_analyzer = LiquidationAnalyzer()
    await liq_analyzer.run()
    liq_analyzer.close()

    # 온체인 데이터 수집
    print('온체인 데이터 수집 중...')
    onchain_collector = OnChainDataCollector()
    await onchain_collector.run()
    onchain_collector.close()

    print('모든 데이터 수집 완료')

asyncio.run(run_all_once())
" 2>&1 | tee -a "$LOG_FILE"

    exit 0
fi

# 스케줄러 실행이 명시적으로 비활성화되지 않은 경우
if [ "$SCHEDULER_ONLY" = true ] || [ "$RESET" = true ] || [ "$BACKFILL" = true ] || [ $# -eq 0 ]; then
    echo "데이터 수집 스케줄러 시작 중..." | tee -a "$LOG_FILE"

    # 실행 로그가 계속 출력되도록 nohup 사용
    nohup python scheduler.py >> "$LOG_FILE" 2>&1 &
    SCHEDULER_PID=$!

    echo "스케줄러가 PID $SCHEDULER_PID로 백그라운드에서 시작되었습니다" | tee -a "$LOG_FILE"
    echo "로그를 보려면 다음 명령을 사용하세요: tail -f $LOG_FILE" | tee -a "$LOG_FILE"
    echo "스케줄러를 종료하려면 다음 명령을 사용하세요: kill -9 $SCHEDULER_PID" | tee -a "$LOG_FILE"

    # PID 저장
    echo "$SCHEDULER_PID" > "$LOG_DIR/scheduler.pid"
fi

echo "=== 설정 완료 (KST: $(TZ='Asia/Seoul' date +"%Y-%m-%d %H:%M:%S")) ===" | tee -a "$LOG_FILE"