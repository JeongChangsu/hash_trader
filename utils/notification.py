# utils/notification.py
import requests
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("notification")


class NotificationSystem:
    def __init__(self, config):
        self.config = config
        self.telegram_token = config.get("telegram_token", "")
        self.telegram_chat_id = config.get("telegram_chat_id", "")
        self.email = config.get("notification_email", "")

    def send_telegram(self, message):
        """텔레그램 알림 전송"""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("텔레그램 설정이 완료되지 않았습니다")
            return False

        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            response = requests.post(url, data=data)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"텔레그램 알림 전송 실패: {e}")
            return False

    def notify_trade(self, trade_data):
        """거래 알림"""
        action = trade_data.get("action", "unknown")
        direction = trade_data.get("direction", "unknown")
        price = trade_data.get("price", 0)

        if action == "entry":
            message = f"🟢 *진입 알림*\n" \
                      f"심볼: {self.config.get('symbol', 'unknown')}\n" \
                      f"방향: {'매수 ⬆️' if direction == 'long' else '매도 ⬇️'}\n" \
                      f"가격: ${price}\n" \
                      f"손절가: ${trade_data.get('stop_loss', 0)}"
        elif action == "exit":
            pnl = trade_data.get("pnl_pct", 0)
            pnl_icon = "🟢" if pnl >= 0 else "🔴"
            message = f"{pnl_icon} *청산 알림*\n" \
                      f"심볼: {self.config.get('symbol', 'unknown')}\n" \
                      f"방향: {'매수 ⬆️' if direction == 'long' else '매도 ⬇️'}\n" \
                      f"가격: ${price}\n" \
                      f"손익: {pnl:.2f}%"
        else:
            message = f"📊 *거래 알림*\n" \
                      f"액션: {action}\n" \
                      f"상세: {trade_data}"

        return self.send_telegram(message)

    def notify_error(self, error_msg, severity="warning"):
        """오류 알림"""
        icon = "⚠️" if severity == "warning" else "🚨"
        message = f"{icon} *오류 알림*\n" \
                  f"{error_msg}"

        return self.send_telegram(message)

    def notify_system_status(self, status_data):
        """시스템 상태 알림"""
        message = f"📈 *시스템 상태*\n" \
                  f"실행 시간: {status_data.get('runtime_hours', 0)}시간\n" \
                  f"거래 횟수: {status_data.get('total_trades', 0)}\n" \
                  f"활성 포지션: {status_data.get('active_positions_count', 0)}\n" \
                  f"현재 잔고: ${status_data.get('current_balance', 0)}"

        return self.send_telegram(message)
