# config/logging_config.py
"""
HashTrader 로깅 설정 모듈

이 모듈은 시스템 전체의 로깅 설정을 관리합니다.
로그 포맷, 로그 레벨, 로그 파일 위치 등을 설정하고
전체 시스템에서 일관된 로깅 방식을 제공합니다.
"""

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Optional, Dict, Any

from config.settings import LOG_LEVEL, LOG_FORMAT, LOG_FILE, LOG_DIR


def configure_logging(
        module_name: str,
        level: Optional[int] = None,
        log_to_file: bool = True,
        log_to_console: bool = True,
        log_file: Optional[str] = None
) -> logging.Logger:
    """
    지정된 모듈에 대한 로거를 구성합니다.

    Args:
        module_name: 로거 이름
        level: 로그 레벨 (기본값: 설정의 LOG_LEVEL)
        log_to_file: 파일에 로그를 기록할지 여부
        log_to_console: 콘솔에 로그를 기록할지 여부
        log_file: 로그 파일 경로 (기본값: 모듈명을 사용한 파일)

    Returns:
        logging.Logger: 구성된 로거
    """
    if level is None:
        level = LOG_LEVEL

    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    # 핸들러가 이미 설정되어 있다면 중복 설정 방지
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT)

    # 콘솔 핸들러 추가
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 파일 핸들러 추가
    if log_to_file:
        if log_file is None:
            log_file = os.path.join(LOG_DIR, f"{module_name.replace('.', '_')}.log")

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def configure_all_loggers(modules: Dict[str, Dict[str, Any]]) -> None:
    """
    여러 모듈의 로거를 일괄 구성합니다.

    Args:
        modules: {모듈명: 설정} 형태의 딕셔너리
    """
    for module_name, config in modules.items():
        configure_logging(module_name, **config)


# 기본 로거 설정
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)

# 로그 디렉토리 생성
os.makedirs(LOG_DIR, exist_ok=True)
