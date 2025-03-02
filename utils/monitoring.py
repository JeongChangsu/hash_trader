# utils/monitoring.py
import time
import logging
from datetime import datetime
import json

logger = logging.getLogger("monitoring")


class TradingMonitor:
    def __init__(self, config):
        self.start_time = datetime.now()
        self.trades = []
        self.active_positions = {}
        self.balance_history = []
        self.errors = []
        self.config = config

    def record_trade(self, trade_data):
        """거래 기록"""
        self.trades.append({
            "timestamp": datetime.now().isoformat(),
            "data": trade_data
        })

    def update_balance(self, balance):
        """잔고 업데이트"""
        self.balance_history.append({
            "timestamp": datetime.now().isoformat(),
            "balance": balance
        })

    def record_error(self, error_msg, severity="warning"):
        """오류 기록"""
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "message": error_msg,
            "severity": severity
        })

    def get_summary(self):
        """현재 상태 요약"""
        current_time = datetime.now()
        runtime = (current_time - self.start_time).total_seconds() / 3600  # 시간 단위

        return {
            "start_time": self.start_time.isoformat(),
            "current_time": current_time.isoformat(),
            "runtime_hours": round(runtime, 2),
            "total_trades": len(self.trades),
            "active_positions_count": len(self.active_positions),
            "initial_balance": self.balance_history[0]["balance"] if self.balance_history else 0,
            "current_balance": self.balance_history[-1]["balance"] if self.balance_history else 0,
            "error_count": len(self.errors)
        }

    def save_to_file(self, filename=None):
        """모니터링 데이터를 파일로 저장"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"monitoring_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump({
                "summary": self.get_summary(),
                "trades": self.trades,
                "balance_history": self.balance_history,
                "errors": self.errors,
                "config": self.config
            }, f, indent=2)

        logger.info(f"모니터링 데이터가 {filename}에 저장되었습니다")