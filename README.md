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