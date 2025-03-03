```aiignore
hash_trader/
│
├── config/
│   ├── settings.py             # 전역 설정
│   ├── exchange_config.py      # 거래소 연결 설정
│   └── logging_config.py       # 로깅 설정
│
├── data_collectors/
│   ├── market_data_collector.py      # 시장 데이터 수집
│   ├── onchain_data_collector.py     # 온체인 데이터 수집
│   ├── liquidation_analyzer.py       # 청산 히트맵 분석
│   ├── backfill_ohlcv.py             # ohlcv 과거 데이터 부족 시 수집
│   └── fear_greed_collector.py       # 공포&탐욕 지수 수집
│
├── analyzers/
│   ├── chart_pattern_analyzer.py     # 차트 패턴 분석
│   ├── multi_timeframe_analyzer.py   # 다중 시간프레임 분석
│   ├── market_condition_classifier.py # 시장 상황 분류
│   └── ai_strategy_selector.py       # AI 전략 선택
│
├── strategies/
│   ├── base_strategy.py              # 기본 전략 클래스
│   ├── trend_following_strategy.py   # 추세추종 전략
│   ├── reversal_strategy.py          # 반전 전략
│   ├── breakout_strategy.py          # 돌파 전략
│   └── range_strategy.py             # 레인지 전략
│
├── execution/
│   ├── entry_exit_manager.py         # 진입/퇴출 관리
│   ├── risk_manager.py               # 위험 관리
│   ├── position_manager.py           # 포지션 관리
│   └── performance_tracker.py        # 성과 추적
│
├── database/
│   ├── redis_handler.py              # Redis 인터페이스
│   └── postgres_handler.py           # PostgreSQL 인터페이스
│
├── utils/
│   ├── monitoring.py                 # 데이터 시각화(궁극적으로 UI 구현이 목표)
│   └── notification.py               # 알림 시스템
│
├── ai/
│   ├── gemini_api.py                 # Gemini API 인터페이스
│   ├── claude_api.py                 # Claude API 인터페이스
│   └── prompt_templates.py           # AI 프롬프트 템플릿
│
├── scripts/
│   ├── setup_database.py             # 데이터베이스 초기화
│   ├── backtest_strategies.py        # 전략 백테스팅
│   └── optimize_parameters.py        # 파라미터 최적화
│
├── logs/                             # 로그 파일 디렉토리
│
├── main.py                           # 메인 실행 파일
├── scheduler.py                      # 스케줄러 (데이터 수집 등)
└── README.md                         # 프로젝트 설명
```

---

# 데이터 수집 가이드

HashTrader는 다양한 데이터 소스에서 거래 결정에 필요한 정보를 수집합니다. 이 섹션에서는 데이터 수집 과정과 각 모듈의 역할을 설명합니다.

## 데이터 수집 모듈

| 모듈 | 주기 | 수집 시간(KST) | 데이터 소스 | 저장 위치 |
|------|------|----------------|------------|----------|
| 시장 데이터 | 1분 | 매분 정각 | Binance | Redis, PostgreSQL |
| 거래량 프로필 | 30분 | 매 30분 | Binance | Redis, PostgreSQL |
| 공포&탐욕 지수 | 1일 1회 | 09:30 | Alternative.me | Redis, PostgreSQL |
| 청산 히트맵 | 1일 3회 | 03:00, 11:00, 19:00 | Coinglass | Redis, PostgreSQL |
| 온체인 데이터 | 1일 3회 | 01:30, 09:30, 17:30 | CryptoQuant | Redis, PostgreSQL |
| 데이터 무결성 | 1일 1회 | 04:00 | 자체 검사 | - |

### 1. 시장 데이터 수집기 (market_data_collector.py)
- **주기**: 60초(1분) 간격으로 실행
- **데이터**: 
  - 다중 시간프레임 OHLCV 데이터 (1m, 5m, 15m, 1h, 4h, 1d, 1w)
  - 오더북 데이터
- **용도**: 기술적 분석, 전략 실행, 진입/퇴출 결정

### 2. 거래량 프로필 분석기
- **주기**: 30분 간격으로 계산
- **데이터**: 
  - 가격별 거래량 분포
  - POC(Point of Control)
  - 가치 영역(Value Area)
- **용도**: 주요 지지/저항 레벨 식별, 거래 구간 파악

### 3. 공포&탐욕 지수 수집기 (fear_greed_collector.py)
- **주기**: 매일 오전 9:30 (KST)
- **데이터**: 
  - Bitcoin Fear & Greed Index
  - 변화율 및 시장 감정 분석
- **용도**: 시장 심리 판단, 극단적 감정 구간 식별

### 4. 청산 히트맵 분석기 (liquidation_analyzer.py)
- **주기**: 하루 3회 - 오전 3시, 오전 11시, 오후 7시 (KST)
- **데이터**: 
  - 비트코인 청산 히트맵
  - 청산 클러스터 및 중요 가격대
- **용도**: 레버리지 포지션 분포 분석, TP/SL 최적화

### 5. 온체인 데이터 수집기 (onchain_data_collector.py)
- **주기**: 하루 3회 - 오전 1:30, 오전 9:30, 오후 5:30 (KST)
- **데이터**: 
  - Exchange Reserve, Netflow
  - MVRV Ratio, Adjusted SOPR
  - 기타 주요 온체인 지표
- **용도**: 장기 추세 분석, 스마트머니 흐름 추적

## 데이터 수집 실행

데이터 수집은 `run_data_collectors.sh` 스크립트를 통해 관리됩니다:

```bash
# 기본 실행 (스케줄러 모드)
./run_data_collectors.sh

# 데이터베이스 초기화 후 실행
./run_data_collectors.sh --reset

# 과거 데이터 백필
./run_data_collectors.sh --backfill --days 90

# 개별 수집기 실행
./run_data_collectors.sh --market-only      # 시장 데이터만
./run_data_collectors.sh --fear-greed-only  # 공포&탐욕 지수만
./run_data_collectors.sh --liquidation-only # 청산 히트맵만
./run_data_collectors.sh --onchain-only     # 온체인 데이터만
./run_data_collectors.sh --volume-only      # 거래량 프로필만

# 모든 수집기 한 번만 실행
./run_data_collectors.sh --run-once
```

## 데이터 저장

수집된 모든 데이터는 다음 위치에 저장됩니다:

1. **Redis**: 실시간 데이터 접근용 (키 형식: `data:market:ohlcv:[exchange]:[symbol]:[timeframe]:[timestamp]`)
2. **PostgreSQL**: 장기 저장 및 분석용 (테이블: `market_ohlcv`, `market_orderbook`, `volume_profile`, `fear_greed_index`, `liquidation_heatmap`, `onchain_data`)

## 주요 설정

데이터 수집 관련 설정은 `config/settings.py`에서 관리됩니다:

```python
# 데이터 수집 주기 설정 (초 단위)
MARKET_DATA_INTERVAL = 60            # 시장 데이터 (1분)
ORDERBOOK_INTERVAL = 5               # 오더북 (5초)
VOLUME_PROFILE_INTERVAL = 1800       # 거래량 프로필 (30분)
LIQUIDATION_HEATMAP_INTERVAL = 14400 # 청산 히트맵 (4시간)
FEAR_GREED_INTERVAL = 43200          # 공포&탐욕 지수 (12시간)
ONCHAIN_DATA_INTERVAL = 21600        # 온체인 데이터 (6시간)
```

## 백필 설정

과거 데이터 수집 시 각 시간프레임별 기본 설정:

- 1분봉: 7일치 데이터
- 5분봉: 30일치 데이터
- 15분봉: 60일치 데이터
- 1시간봉: 180일치 데이터
- 4시간봉: 365일치 데이터
- 일봉: 730일치 데이터 (2년)
- 주봉: 520일치 데이터 (10년)

백필 일수는 `--days` 옵션으로 조정할 수 있습니다.
