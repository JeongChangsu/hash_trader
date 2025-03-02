# config/settings.py
"""
HashTrader 전역 설정 모듈

이 모듈은 HashTrader 시스템의 전역 설정을 관리합니다.
환경 변수, 경로, API 키, 데이터베이스 설정 등 시스템 전체에서 사용되는
모든 설정 값들을 중앙집중식으로 관리합니다.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

# 프로젝트 기본 경로
BASE_DIR = Path(__file__).resolve().parent.parent

# 로깅 설정
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOG_FILE = os.path.join(LOG_DIR, "hash_trader.log")

# 데이터베이스 설정
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db": int(os.getenv("REDIS_DB", 0)),
    "password": os.getenv("REDIS_PASSWORD", None),
    "decode_responses": True
}

POSTGRES_CONFIG = {
    "dbname": os.getenv("POSTGRES_DBNAME", "magok_trader"),
    "user": os.getenv("POSTGRES_USER", "hashmar"),
    "password": os.getenv("POSTGRES_PASSWORD", "1111"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432))
}

# 거래소 설정
DEFAULT_EXCHANGE = "binance"
EXCHANGE_CONFIGS = {
    "binance": {
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "enableRateLimit": True,
        "options": {
            "defaultType": "future"
        }
    }
}

# 트레이딩 설정
TRADING_SYMBOLS = ["BTC/USDT"]
TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]

# AI API 설정
GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY", "")
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GPT_API_KEY = os.getenv("OPENAI_API_KEY", "")

# 데이터 수집 주기 설정 (초 단위)
MARKET_DATA_INTERVAL = 60  # 1분
ORDERBOOK_INTERVAL = 15  # 15초
VOLUME_PROFILE_INTERVAL = 3600  # 1시간
LIQUIDATION_HEATMAP_INTERVAL = 86400  # 24시간
FEAR_GREED_INTERVAL = 86400  # 24시간
ONCHAIN_DATA_INTERVAL = 86400  # 24시간

# 전략 설정
DEFAULT_RISK_PER_TRADE = 0.01  # 거래당 계정 자본의 1%
MAX_LEVERAGE = 10  # 최대 레버리지
MAX_OPEN_POSITIONS = 1  # 동시 오픈 포지션 수 제한
DEFAULT_TRADE_SIZE_USD = 3000  # 기본 거래 규모 (USD)

# 청산 히트맵 분석 설정
HEATMAP_IMAGES_DIR = os.path.expanduser('~/liquidation_heatmap_imgs')
os.makedirs(HEATMAP_IMAGES_DIR, exist_ok=True)

# 기술적 지표 설정
INDICATOR_SETTINGS = {
    "RSI": {
        "length": 14,
        "overbought": 70,
        "oversold": 30
    },
    "MACD": {
        "fast_length": 12,
        "slow_length": 26,
        "signal_length": 9
    },
    "Bollinger": {
        "length": 20,
        "multiplier": 2
    },
    "ATR": {
        "length": 14
    }
}

# 중요 온체인 지표 목록
KEY_ONCHAIN_METRICS = [
    "Exchange Reserve",
    "Exchange Netflow",
    "Exchange Inflow Top10",
    "Exchange Outflow Top10",
    "Miner Position Index (MPI)",
    "Exchange Whale Ratio",
    "Miner Reserve",
    "Adjusted SOPR",
    "MVRV Ratio",
    "Coinbase Premium",
    "Net Unrealized Profit/Loss (NUPL)"
]

# 전략과 관련된 설정
STRATEGY_WEIGHTS = {
    "trend_following": 0.35,
    "reversal": 0.25,
    "breakout": 0.25,
    "range": 0.15
}

# 시스템 상태 및 성능 모니터링 설정
MONITOR_HEARTBEAT_INTERVAL = 300  # 시스템 상태 확인 주기 (5분)
