# config/exchange_config.py
"""
거래소 연결 설정 모듈

이 모듈은 지원되는 모든 거래소에 대한 설정을 관리하고
거래소 인스턴스를 초기화하는 함수를 제공합니다.
"""

import os
import ccxt.pro as ccxtpro
import logging
from typing import Dict, Optional, Any

from config.settings import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)


async def create_exchange_instance(
        exchange_id: str,
        config: Optional[Dict[str, Any]] = None
) -> ccxtpro.Exchange:
    """
    지정된 ID의 거래소 인스턴스를 생성합니다.

    Args:
        exchange_id: 거래소 ID
        config: 거래소별 설정(지정되지 않으면 기본 설정 사용)

    Returns:
        ccxtpro.Exchange: 거래소 인스턴스
    """
    if exchange_id not in ccxtpro.exchanges:
        raise ValueError(f"지원되지 않는 거래소: {exchange_id}")

    # 기본 설정과 사용자 설정 병합
    exchange_config = EXCHANGE_CONFIGS.get(exchange_id, {}).copy()
    if config:
        exchange_config.update(config)

    exchange_class = getattr(ccxtpro, exchange_id)
    exchange = exchange_class(exchange_config)

    logger.info(f"{exchange_id} 거래소 인스턴스 생성됨")

    try:
        await exchange.load_markets()
        logger.info(f"{exchange_id} 시장 데이터 로드됨")
    except Exception as e:
        logger.error(f"{exchange_id} 시장 데이터 로드 실패: {e}")
        await exchange.close()
        raise

    return exchange


def get_supported_exchanges() -> Dict[str, Dict]:
    """
    지원되는 모든 거래소와 그 설정을 반환합니다.

    Returns:
        Dict[str, Dict]: {거래소 ID: 설정} 형태의 딕셔너리
    """
    return {
        exchange_id: config
        for exchange_id, config in EXCHANGE_CONFIGS.items()
        if exchange_id in ccxtpro.exchanges
    }


def validate_exchange_config(exchange_id: str) -> bool:
    """
    거래소 설정이 유효한지 검증합니다.

    Args:
        exchange_id: 거래소 ID

    Returns:
        bool: 설정이 유효하면 True, 그렇지 않으면 False
    """
    if exchange_id not in EXCHANGE_CONFIGS:
        logger.warning(f"설정에 {exchange_id} 거래소가 없습니다")
        return False

    config = EXCHANGE_CONFIGS[exchange_id]

    # API 키가 필요한 거래소인 경우 검증
    if 'apiKey' in config and not config['apiKey']:
        logger.warning(f"{exchange_id} API 키가 설정되지 않았습니다")
        return False

    if 'secret' in config and not config['secret']:
        logger.warning(f"{exchange_id} API 시크릿이 설정되지 않았습니다")
        return False

    return True
