# execution/position_manager.py
"""
포지션 관리자

이 모듈은 거래소 인터페이스와 통신하여 실제 주문을 실행하고
활성 포지션을 관리합니다. 진입, 퇴출, 부분 청산 등의 기능을 제공합니다.
"""

import asyncio
import logging
import json
import redis
import psycopg2
import ccxt.pro as ccxtpro
import time
import uuid
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, timedelta

from config.settings import REDIS_CONFIG, POSTGRES_CONFIG, DEFAULT_EXCHANGE
from config.logging_config import configure_logging
from config.exchange_config import create_exchange_instance


class PositionManager:
    """
    거래소 주문 실행 및 포지션 관리를 처리하는 클래스
    """

    def __init__(
            self,
            symbol: str = "BTC/USDT",
            exchange_id: str = DEFAULT_EXCHANGE,
            config: Optional[Dict[str, Any]] = None
    ):
        """
        PositionManager 초기화

        Args:
            symbol: 거래 심볼
            exchange_id: 거래소 ID
            config: 포지션 관리 설정
        """
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.logger = configure_logging("position_manager")

        # Redis 연결
        self.redis_client = redis.Redis(**REDIS_CONFIG)

        # PostgreSQL 연결
        self.db_conn = psycopg2.connect(**POSTGRES_CONFIG)

        # 기본 설정
        self.default_config = {
            "order_type": "market",  # 기본 주문 유형 (market 또는 limit)
            "limit_price_buffer": 0.1,  # 지정가 주문의 가격 버퍼 (%)
            "partial_tp_enabled": True,  # 부분 이익실현 활성화
            "retry_attempts": 3,  # 주문 실패 시 재시도 횟수
            "retry_delay": 2,  # 재시도 간 지연 시간(초)
            "max_slippage": 0.5,  # 최대 허용 슬리피지 (%)
            "use_post_only": True,  # 포스트 온리 주문 사용
            "use_reduce_only": True,  # 리듀스 온리 주문 사용 (청산)
            "verify_order": True,  # 주문 실행 후 검증
            "use_oco_orders": False,  # OCO 주문 사용
            "use_trailing_stop": False,  # 트레일링 스탑 사용
            "cancel_after_timeout": 60,  # 주문 타임아웃(초)
            "max_open_orders": 20,  # 최대 미체결 주문 수
            "emergency_close_retry": 5,  # 비상 청산 시 재시도 횟수
            "position_update_interval": 60,  # 포지션 업데이트 간격(초)
            "order_update_interval": 30,  # 주문 업데이트 간격(초)
            "tp_sl_adjustment_interval": 300,  # TP/SL 조정 간격(초)
            "auto_reduce_highly_profitable": True,  # 고수익 포지션 자동 축소
            "high_profit_threshold": 30,  # 고수익 임계값 (%)
            "high_profit_reduce_percent": 50,  # 고수익 시 축소 비율 (%)
            "custom_symbol_settings": {},  # 심볼별 맞춤 설정
            "simulated_mode": False,  # 시뮬레이션 모드(주문 실행 없음)
            "exclude_executions_in_test": False  # 테스트에서 주문 실행 제외
        }

        # 사용자 설정으로 업데이트
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)

        # 심볼별 맞춤 설정 적용
        symbol_config = self.config["custom_symbol_settings"].get(symbol, {})
        if symbol_config:
            for key, value in symbol_config.items():
                self.config[key] = value

        # 거래소 인스턴스
        self.exchange = None

        # 활성 포지션 캐시
        self.active_positions = {}

        # 미체결 주문 캐시
        self.open_orders = {}

        # 거래소 정보 캐시
        self.exchange_info = {}

        # 활성 작업
        self.active_tasks = {}

        self.logger.info(f"PositionManager 초기화됨: {symbol}, 거래소: {exchange_id}")

    async def initialize(self) -> bool:
        """
        포지션 관리자를 초기화하고 거래소에 연결합니다.

        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 거래소 인스턴스 생성
            self.exchange = await create_exchange_instance(self.exchange_id)

            # 거래소 정보 로드
            await self.load_exchange_info()

            # 활성 포지션 및 주문 로드
            await self.load_positions()
            await self.load_open_orders()

            self.logger.info(f"포지션 관리자 초기화 완료: {self.exchange_id}")
            return True

        except Exception as e:
            self.logger.error(f"포지션 관리자 초기화 실패: {e}")
            return False

    async def load_exchange_info(self) -> None:
        """거래소 정보를 로드합니다."""
        try:
            # 시장 데이터 로드
            await self.exchange.load_markets()

            # 심볼 정보 확인
            if self.symbol not in self.exchange.markets:
                raise ValueError(f"거래소에서 {self.symbol} 심볼을 찾을 수 없습니다")

            # 심볼 정보 저장
            self.exchange_info = {
                "market_info": self.exchange.markets[self.symbol],
                "limits": self.exchange.markets[self.symbol].get("limits", {}),
                "precision": self.exchange.markets[self.symbol].get("precision", {}),
                "fee_structure": self.exchange.markets[self.symbol].get("fee", {})
            }

            # 거래소 제한사항 로그
            limits = self.exchange_info["limits"]
            self.logger.info(
                f"거래소 제한: 최소 금액={limits.get('cost', {}).get('min', 'N/A')}, "
                f"최소 수량={limits.get('amount', {}).get('min', 'N/A')}"
            )

        except Exception as e:
            self.logger.error(f"거래소 정보 로드 실패: {e}")
            raise

    async def load_positions(self) -> None:
        """활성 포지션을 로드합니다."""
        try:
            if self.exchange.has['fetchPositions']:
                positions = await self.exchange.fetch_positions([self.symbol]) or []

                # 포지션 정보 저장
                self.active_positions = {}
                for position in positions:
                    if position['symbol'] == self.symbol and float(position['contracts']) > 0:
                        position_id = position.get('id', str(uuid.uuid4()))
                        self.active_positions[position_id] = position

                self.logger.info(f"포지션 로드 완료: {len(self.active_positions)}개 활성 포지션")
            else:
                self.logger.warning(f"{self.exchange_id} 거래소는 포지션 조회를 지원하지 않습니다")

        except Exception as e:
            self.logger.error(f"포지션 로드 실패: {e}")

    async def load_open_orders(self) -> None:
        """미체결 주문을 로드합니다."""
        try:
            orders = await self.exchange.fetch_open_orders(self.symbol) or []

            # 주문 정보 저장
            self.open_orders = {}
            for order in orders:
                order_id = order.get('id')
                if order_id:
                    self.open_orders[order_id] = order

            self.logger.info(f"주문 로드 완료: {len(self.open_orders)}개 미체결 주문")

        except Exception as e:
            self.logger.error(f"미체결 주문 로드 실패: {e}")

    async def create_position(
            self,
            direction: str,
            size: float,
            entry_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        새로운 포지션을 생성합니다.

        Args:
            direction: 포지션 방향 ('long' 또는 'short')
            size: 포지션 크기 (코인 수량)
            entry_params: 진입 파라미터

        Returns:
            Dict: 포지션 생성 결과
        """
        self.logger.info(f"포지션 생성 시작: {direction}, 크기: {size}")

        # 기본 결과 구조
        result = {
            "success": False,
            "position_id": "",
            "order_ids": [],
            "position_size": size,
            "entry_price": 0,
            "direction": direction,
            "timestamp": int(time.time() * 1000),
            "orders": [],
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: 포지션 생성 시뮬레이션")
            result.update({
                "success": True,
                "position_id": str(uuid.uuid4()),
                "entry_price": entry_params.get("entry_price", 0),
                "simulated": True
            })
            return result

        try:
            # 1. 주문 파라미터 준비
            side = "buy" if direction == "long" else "sell"

            order_type = entry_params.get("order_type", self.config["order_type"])
            leverage = entry_params.get("leverage", 1)

            # 2. 레버리지 설정
            if hasattr(self.exchange, 'set_leverage'):
                try:
                    await self.exchange.set_leverage(leverage, self.symbol)
                    self.logger.info(f"레버리지 설정: {leverage}x")
                except Exception as e:
                    self.logger.warning(f"레버리지 설정 실패: {e}")

            # 3. 주문 파라미터 구성
            params = {
                "leverage": leverage
            }

            # 포스트 온리 주문 (지정가 주문인 경우만)
            if order_type == "limit" and self.config["use_post_only"]:
                params["postOnly"] = True

            # 4. 가격 계산
            price = None
            if order_type == "limit":
                current_price = await self.get_current_price()

                # 방향에 따른 가격 조정 (살 때는 조금 높게, 팔 때는 조금 낮게)
                buffer = self.config["limit_price_buffer"] / 100
                if direction == "long":
                    price = current_price * (1 - buffer)  # 약간 낮은 가격에 매수
                else:
                    price = current_price * (1 + buffer)  # 약간 높은 가격에 매도

            # 5. 주문 실행
            order = None
            for attempt in range(self.config["retry_attempts"]):
                try:
                    if order_type == "market":
                        order = await self.exchange.create_order(
                            self.symbol,
                            'market',
                            side,
                            size,
                            None,
                            params
                        )
                    else:  # limit
                        order = await self.exchange.create_order(
                            self.symbol,
                            'limit',
                            side,
                            size,
                            price,
                            params
                        )

                    break  # 성공하면 루프 종료

                except Exception as e:
                    self.logger.error(f"주문 실행 실패 (시도 {attempt + 1}/{self.config['retry_attempts']}): {e}")
                    if attempt == self.config["retry_attempts"] - 1:
                        raise  # 모든 시도 실패
                    await asyncio.sleep(self.config["retry_delay"])

            if not order:
                raise Exception("주문을 생성할 수 없습니다")

            # 6. 주문 검증
            if self.config["verify_order"] and order_type == "market":
                await self.verify_order_execution(order)

            # 7. 주문 정보 저장
            order_id = order.get('id')
            if order_id:
                self.open_orders[order_id] = order

            # 8. 진입에 성공한 경우 TP/SL 주문 생성
            position_id = str(uuid.uuid4())
            result.update({
                "success": True,
                "position_id": position_id,
                "order_ids": [order.get('id')],
                "entry_price": order.get('price') or entry_params.get("entry_price", 0),
                "orders": [order]
            })

            # 9. TP/SL 설정
            if entry_params.get("stop_loss") or entry_params.get("take_profit_levels"):
                tp_sl_result = await self.set_tp_sl_orders(
                    position_id,
                    direction,
                    size,
                    result["entry_price"],
                    entry_params.get("stop_loss"),
                    entry_params.get("take_profit_levels"),
                    entry_params.get("tp_percentages", [])
                )

                # TP/SL 주문 ID 추가
                result["order_ids"].extend(tp_sl_result.get("order_ids", []))
                result["orders"].extend(tp_sl_result.get("orders", []))

            # 10. 포지션 추적에 추가
            self.active_positions[position_id] = {
                "position_id": position_id,
                "symbol": self.symbol,
                "direction": direction,
                "size": size,
                "entry_price": result["entry_price"],
                "leverage": leverage,
                "entry_order_id": order.get('id'),
                "tp_order_ids": [o.get('id') for o in result["orders"] if o.get('side') != side],
                "sl_order_id": next(
                    (o.get('id') for o in result["orders"] if o.get('type') == 'stop' and o.get('side') != side), None),
                "status": "open",
                "entry_time": order.get('timestamp') or int(time.time() * 1000),
                "last_update_time": int(time.time() * 1000),
                "unrealized_pnl": 0,
                "realized_pnl": 0
            }

            # 11. 포지션 생성 정보 Redis에 저장
            await self.save_position_to_redis(position_id)

            # 12. 데이터베이스에 기록
            await self.record_position_in_db(position_id, "create")

            self.logger.info(
                f"포지션 생성 완료: "
                f"ID={position_id}, "
                f"진입가={result['entry_price']}, "
                f"크기={size}, "
                f"레버리지={leverage}x"
            )

        except Exception as e:
            error_msg = f"포지션 생성 실패: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def verify_order_execution(self, order: Dict[str, Any]) -> None:
        """
        주문 실행을 검증합니다.

        Args:
            order: 주문 정보
        """
        order_id = order.get('id')
        if not order_id:
            self.logger.warning("주문 ID가 없어 검증을 건너뜁니다")
            return

        try:
            # 처음 몇 번은 빠르게 확인, 그 후 더 긴 간격으로 확인
            checks = [0.5, 1, 2, 3, 5, 10]

            for wait_time in checks:
                await asyncio.sleep(wait_time)

                # 주문 상태 확인
                fetched_order = await self.exchange.fetch_order(order_id)

                status = fetched_order.get('status', '')
                filled = fetched_order.get('filled', 0)
                amount = fetched_order.get('amount', 0)

                # 완전히 체결됐는지 확인
                if status == 'closed' or (filled and filled >= amount * 0.99):
                    self.logger.info(f"주문 완전히 체결됨: {order_id}, 수량: {filled}/{amount}")
                    return

                # 부분 체결 확인
                if filled > 0:
                    self.logger.info(f"주문 부분 체결 중: {order_id}, 수량: {filled}/{amount}")

                # 주문이 취소됐는지 확인
                if status == 'canceled':
                    self.logger.warning(f"주문 취소됨: {order_id}")
                    raise Exception(f"주문이 취소되었습니다: {order_id}")

            # 시간 초과 후에도 완전히 체결되지 않았다면
            self.logger.warning(f"주문 체결 시간 초과: {order_id}, 상태: {status}")

            # 미체결 주문 처리 (설정에 따라 취소 또는 유지)
            if self.config.get("cancel_after_timeout", False):
                await self.exchange.cancel_order(order_id)
                self.logger.info(f"시간 초과로 주문 취소됨: {order_id}")
                raise Exception(f"주문 체결 시간 초과로 취소됨: {order_id}")

        except Exception as e:
            self.logger.error(f"주문 검증 중 오류 발생: {e}")
            raise

    async def set_tp_sl_orders(
            self,
            position_id: str,
            direction: str,
            size: float,
            entry_price: float,
            stop_loss: Optional[float] = None,
            take_profit_levels: Optional[List[float]] = None,
            tp_percentages: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """
        이익실현(TP)과 손절(SL) 주문을 설정합니다.

        Args:
            position_id: 포지션 ID
            direction: 포지션 방향 ('long' 또는 'short')
            size: 포지션 크기 (코인 수량)
            entry_price: 진입 가격
            stop_loss: 손절 가격
            take_profit_levels: 이익실현 레벨 목록
            tp_percentages: 각 이익실현 레벨에 할당할 포지션 비율

        Returns:
            Dict: TP/SL 주문 설정 결과
        """
        self.logger.info(f"TP/SL 주문 설정: 포지션={position_id}, 방향={direction}")

        # 기본 결과 구조
        result = {
            "success": False,
            "order_ids": [],
            "orders": [],
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: TP/SL 주문 설정 시뮬레이션")
            result["success"] = True
            result["simulated"] = True
            return result

        try:
            # 1. 주문 파라미터 준비
            close_side = "sell" if direction == "long" else "buy"

            # 2. 기본 매개변수 설정
            params = {
                "reduceOnly": True if self.config["use_reduce_only"] else False
            }

            orders = []

            # 3. 손절 주문 생성
            if stop_loss is not None and stop_loss > 0:
                stop_side = close_side

                try:
                    stop_order = await self.exchange.create_order(
                        self.symbol,
                        'stop',  # 또는 'stop_loss', 거래소에 따라 다름
                        stop_side,
                        size,
                        None,  # 가격은 stop_price에 따라 결정
                        {
                            **params,
                            'stopPrice': stop_loss
                        }
                    )

                    orders.append(stop_order)
                    self.logger.info(f"손절 주문 생성됨: 가격={stop_loss}, ID={stop_order.get('id')}")

                    # 미체결 주문에 추가
                    order_id = stop_order.get('id')
                    if order_id:
                        self.open_orders[order_id] = stop_order

                except Exception as e:
                    self.logger.error(f"손절 주문 생성 실패: {e}")

            # 4. 이익실현 주문 생성
            if take_profit_levels and len(take_profit_levels) > 0:
                # 비율 확인 및 기본값 설정
                if not tp_percentages or len(tp_percentages) != len(take_profit_levels):
                    # 균등 분할 (마지막 레벨에 나머지 할당)
                    count = len(take_profit_levels)
                    tp_percentages = [1 / count] * (count - 1)
                    tp_percentages.append(1 - sum(tp_percentages))

                # 각 TP 레벨에 대한 주문 생성
                remaining_size = size

                for i, (tp_price, tp_percent) in enumerate(zip(take_profit_levels, tp_percentages)):
                    # 마지막 이익실현 주문은 남은 전체 수량 사용
                    tp_size = size * tp_percent if i < len(take_profit_levels) - 1 else remaining_size
                    tp_size = max(0, min(tp_size, remaining_size))  # 유효 범위 확인

                    if tp_size <= 0:
                        continue

                    try:
                        tp_order = await self.exchange.create_order(
                            self.symbol,
                            'limit',
                            close_side,
                            tp_size,
                            tp_price,
                            params
                        )

                        orders.append(tp_order)
                        self.logger.info(
                            f"이익실현 주문 생성됨: "
                            f"레벨={i + 1}/{len(take_profit_levels)}, "
                            f"가격={tp_price}, "
                            f"수량={tp_size}, "
                            f"ID={tp_order.get('id')}"
                        )

                        # 미체결 주문에 추가
                        order_id = tp_order.get('id')
                        if order_id:
                            self.open_orders[order_id] = tp_order

                        # 남은 수량 업데이트
                        remaining_size -= tp_size

                    except Exception as e:
                        self.logger.error(f"이익실현 주문 생성 실패 (레벨 {i + 1}): {e}")

            # 5. 결과 업데이트
            result.update({
                "success": len(orders) > 0,
                "order_ids": [order.get('id') for order in orders if order.get('id')],
                "orders": orders
            })

        except Exception as e:
            error_msg = f"TP/SL 주문 설정 실패: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def update_position_orders(
            self,
            position_id: str,
            updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        기존 포지션의 주문을 업데이트합니다.

        Args:
            position_id: 포지션 ID
            updates: 업데이트할 내용

        Returns:
            Dict: 업데이트 결과
        """
        self.logger.info(f"포지션 주문 업데이트: ID={position_id}")

        # 기본 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "cancelled_orders": [],
            "new_orders": [],
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: 포지션 주문 업데이트 시뮬레이션")
            result["success"] = True
            result["simulated"] = True
            return result

        try:
            # 1. 포지션 존재 확인
            if position_id not in self.active_positions:
                raise ValueError(f"ID가 {position_id}인 활성 포지션을 찾을 수 없습니다")

            position = self.active_positions[position_id]
            direction = position.get("direction", "long")
            size = position.get("size", 0)
            entry_price = position.get("entry_price", 0)

            # 2. 기존 TP/SL 주문 취소
            cancelled_orders = []

            # SL 주문 취소
            sl_order_id = position.get("sl_order_id")
            if sl_order_id and sl_order_id in self.open_orders:
                try:
                    cancel_result = await self.exchange.cancel_order(sl_order_id)
                    cancelled_orders.append(cancel_result)
                    self.logger.info(f"기존 SL 주문 취소됨: {sl_order_id}")

                    # 미체결 주문에서 제거
                    if sl_order_id in self.open_orders:
                        del self.open_orders[sl_order_id]

                except Exception as e:
                    self.logger.warning(f"SL 주문 취소 실패: {e}")

            # TP 주문 취소
            tp_order_ids = position.get("tp_order_ids", [])
            for tp_id in tp_order_ids:
                if tp_id in self.open_orders:
                    try:
                        cancel_result = await self.exchange.cancel_order(tp_id)
                        cancelled_orders.append(cancel_result)
                        self.logger.info(f"기존 TP 주문 취소됨: {tp_id}")

                        # 미체결 주문에서 제거
                        if tp_id in self.open_orders:
                            del self.open_orders[tp_id]

                    except Exception as e:
                        self.logger.warning(f"TP 주문 취소 실패: {e}")

            # 3. 새 TP/SL 주문 생성
            new_stop_loss = updates.get("stop_loss")
            new_take_profit_levels = updates.get("take_profit_levels")
            new_tp_percentages = updates.get("tp_percentages")

            # 현재 포지션 크기 확인 (일부 청산된 경우를 고려)
            current_size = await self.get_position_size(position_id)

            if current_size > 0:
                tp_sl_result = await self.set_tp_sl_orders(
                    position_id,
                    direction,
                    current_size,
                    entry_price,
                    new_stop_loss,
                    new_take_profit_levels,
                    new_tp_percentages
                )

                # 새 TP/SL 주문 추가
                new_orders = tp_sl_result.get("orders", [])

                # 포지션 정보 업데이트
                if tp_sl_result.get("success", False):
                    self.active_positions[position_id].update({
                        "tp_order_ids": [o.get('id') for o in new_orders if o.get('type') != 'stop'],
                        "sl_order_id": next((o.get('id') for o in new_orders if o.get('type') == 'stop'), None),
                        "last_update_time": int(time.time() * 1000)
                    })

                    # Redis 업데이트
                    await self.save_position_to_redis(position_id)

                    result.update({
                        "success": True,
                        "cancelled_orders": cancelled_orders,
                        "new_orders": new_orders
                    })

                    self.logger.info(
                        f"포지션 주문 업데이트 완료: "
                        f"ID={position_id}, "
                        f"취소됨={len(cancelled_orders)}, "
                        f"새로 생성됨={len(new_orders)}"
                    )
                else:
                    result["error"] = tp_sl_result.get("error", "알 수 없는 오류")
            else:
                self.logger.warning(f"포지션 크기가 0이거나 음수입니다: {current_size}")
                result["error"] = "포지션 크기가 유효하지 않습니다"

        except Exception as e:
            error_msg = f"포지션 주문 업데이트 실패: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def close_position(
            self,
            position_id: str,
            amount: Optional[float] = None,
            market_close: bool = True
    ) -> Dict[str, Any]:
        """
        포지션을 청산합니다.

        Args:
            position_id: 포지션 ID
            amount: 청산할 수량 (None이면 전체 청산)
            market_close: 시장가 청산 여부

        Returns:
            Dict: 청산 결과
        """
        self.logger.info(f"포지션 청산 시작: ID={position_id}, 수량={amount}, 시장가={market_close}")

        # 기본 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "order_ids": [],
            "orders": [],
            "partial_close": amount is not None,
            "amount_closed": 0,
            "close_price": 0,
            "pnl": 0,
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: 포지션 청산 시뮬레이션")
            result.update({
                "success": True,
                "simulated": True,
                "amount_closed": amount or 0,
                "close_price": await self.get_current_price()
            })
            return result

        try:
            # 1. 포지션 존재 확인
            if position_id not in self.active_positions:
                raise ValueError(f"ID가 {position_id}인 활성 포지션을 찾을 수 없습니다")

            position = self.active_positions[position_id]
            direction = position.get("direction", "long")
            position_size = position.get("size", 0)
            entry_price = position.get("entry_price", 0)

            # 현재 크기 확인
            current_size = await self.get_position_size(position_id)

            # 청산할 수량 결정
            close_amount = amount if amount is not None else current_size
            close_amount = min(close_amount, current_size)

            if close_amount <= 0:
                raise ValueError(f"청산할 수량이 유효하지 않습니다: {close_amount}")

            # 부분 청산 여부
            is_partial = close_amount < current_size

            # 2. 청산용 주문 생성
            close_side = "sell" if direction == "long" else "buy"
            order_type = "market" if market_close else "limit"

            # 가격 결정
            price = None
            if not market_close:
                # 현재 가격 조회
                current_price = await self.get_current_price()

                # 방향에 따른 가격 설정
                if direction == "long":
                    price = current_price * 0.995  # 약간 낮게 설정
                else:
                    price = current_price * 1.005  # 약간 높게 설정

            # 주문 매개변수
            params = {
                "reduceOnly": True if self.config["use_reduce_only"] else False
            }

            # 기존 TP/SL 주문 취소
            if not is_partial:
                await self.cancel_position_orders(position_id)

            # 3. 주문 실행
            orders = []

            for attempt in range(self.config["retry_attempts"]):
                try:
                    order = None

                    if order_type == "market":
                        order = await self.exchange.create_order(
                            self.symbol,
                            'market',
                            close_side,
                            close_amount,
                            None,
                            params
                        )
                    else:
                        order = await self.exchange.create_order(
                            self.symbol,
                            'limit',
                            close_side,
                            close_amount,
                            price,
                            params
                        )

                    orders.append(order)

                    # 주문 검증 (시장가인 경우만)
                    if market_close:
                        await self.verify_order_execution(order)

                    # 체결가 확인
                    close_price = order.get('price') or order.get('average') or await self.get_current_price()

                    # 손익 계산
                    if direction == "long":
                        pnl = (close_price - entry_price) * close_amount
                    else:
                        pnl = (entry_price - close_price) * close_amount

                    # 4. 포지션 업데이트 또는 제거
                    if is_partial:
                        # 부분 청산: 포지션 크기 업데이트
                        self.active_positions[position_id].update({
                            "size": current_size - close_amount,
                            "realized_pnl": position.get("realized_pnl", 0) + pnl,
                            "last_update_time": int(time.time() * 1000)
                        })

                        # Redis 업데이트
                        await self.save_position_to_redis(position_id)
                    else:
                        # 전체 청산: 포지션 제거
                        self.active_positions[position_id].update({
                            "status": "closed",
                            "exit_time": order.get('timestamp') or int(time.time() * 1000),
                            "exit_price": close_price,
                            "realized_pnl": position.get("realized_pnl", 0) + pnl,
                            "last_update_time": int(time.time() * 1000)
                        })

                        # 데이터베이스에 기록
                        await self.record_position_in_db(position_id, "close")

                        # 액티브 포지션에서 제거 (선택적)
                        # del self.active_positions[position_id]

                    result.update({
                        "success": True,
                        "order_ids": [o.get('id') for o in orders if o.get('id')],
                        "orders": orders,
                        "amount_closed": close_amount,
                        "close_price": close_price,
                        "pnl": pnl
                    })

                    self.logger.info(
                        f"포지션 청산 완료: "
                        f"ID={position_id}, "
                        f"청산량={close_amount}, "
                        f"가격={close_price}, "
                        f"손익=${pnl:.2f}"
                    )

                    break  # 성공하면 루프 종료

                except Exception as e:
                    self.logger.error(f"청산 시도 실패 (시도 {attempt + 1}/{self.config['retry_attempts']}): {e}")
                    if attempt == self.config["retry_attempts"] - 1:
                        raise  # 모든 시도 실패
                    await asyncio.sleep(self.config["retry_delay"])

        except Exception as e:
            error_msg = f"포지션 청산 실패: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def emergency_close_all_positions(self) -> Dict[str, Any]:
        """
        모든 활성 포지션을 긴급 청산합니다.

        Returns:
            Dict: 청산 결과
        """
        self.logger.warning("모든 포지션 긴급 청산 시작")

        # 기본 결과 구조
        result = {
            "success": False,
            "positions_closed": [],
            "positions_failed": [],
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: 모든 포지션 긴급 청산 시뮬레이션")
            result.update({
                "success": True,
                "simulated": True
            })
            return result

        try:
            # 직접 거래소 포지션 조회
            if self.exchange.has['fetchPositions']:
                positions = await self.exchange.fetch_positions([self.symbol]) or []

                closed = []
                failed = []

                for pos in positions:
                    if pos['symbol'] == self.symbol and float(pos['contracts']) > 0:
                        position_id = pos.get('id', str(uuid.uuid4()))

                        # 긴급 청산 - 다양한 방법 시도
                        success = False

                        # 방법 1: 거래소의 포지션 청산 API 사용 (지원 시)
                        if hasattr(self.exchange, 'close_position'):
                            for attempt in range(self.config["emergency_close_retry"]):
                                try:
                                    await self.exchange.close_position(self.symbol)
                                    success = True
                                    break
                                except Exception as e:
                                    self.logger.error(f"거래소 API 사용 청산 실패 (시도 {attempt + 1}): {e}")
                                    await asyncio.sleep(1)

                        # 방법 2: 시장가 반대 주문 생성
                        if not success:
                            try:
                                side = "sell" if pos['side'].lower() == 'long' else "buy"
                                amount = float(pos['contracts'])

                                for attempt in range(self.config["emergency_close_retry"]):
                                    try:
                                        order = await self.exchange.create_order(
                                            self.symbol,
                                            'market',
                                            side,
                                            amount,
                                            None,
                                            {'reduceOnly': True}
                                        )
                                        success = True
                                        break
                                    except Exception as e:
                                        self.logger.error(f"시장가 청산 실패 (시도 {attempt + 1}): {e}")
                                        await asyncio.sleep(1)

                            except Exception as e:
                                self.logger.error(f"긴급 청산 실패: {e}")

                        # 결과 추적
                        if success:
                            closed.append(position_id)
                            self.logger.info(f"포지션 긴급 청산 성공: {position_id}")
                        else:
                            failed.append(position_id)
                            self.logger.error(f"포지션 긴급 청산 실패: {position_id}")

                # 내부 포지션 추적 업데이트
                for pos_id in closed:
                    if pos_id in self.active_positions:
                        self.active_positions[pos_id].update({
                            "status": "closed",
                            "exit_time": int(time.time() * 1000),
                            "last_update_time": int(time.time() * 1000),
                            "emergency_closed": True
                        })

                result.update({
                    "success": len(failed) == 0,
                    "positions_closed": closed,
                    "positions_failed": failed
                })

                self.logger.info(f"긴급 청산 완료: 성공={len(closed)}, 실패={len(failed)}")

            else:
                self.logger.warning(f"{self.exchange_id} 거래소는 포지션 조회를 지원하지 않습니다")
                result["error"] = "거래소가 포지션 조회를 지원하지 않습니다"

        except Exception as e:
            error_msg = f"긴급 청산 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def cancel_position_orders(self, position_id: str) -> Dict[str, Any]:
        """
        포지션과 연결된 모든 주문을 취소합니다.

        Args:
            position_id: 포지션 ID

        Returns:
            Dict: 취소 결과
        """
        self.logger.info(f"포지션 주문 취소: ID={position_id}")

        # 기본 결과 구조
        result = {
            "success": False,
            "position_id": position_id,
            "cancelled_orders": [],
            "error": ""
        }

        if self.config["simulated_mode"]:
            self.logger.info("시뮬레이션 모드: 포지션 주문 취소 시뮬레이션")
            result["success"] = True
            result["simulated"] = True
            return result

        try:
            # 1. 포지션 존재 확인
            if position_id not in self.active_positions:
                raise ValueError(f"ID가 {position_id}인 활성 포지션을 찾을 수 없습니다")

            position = self.active_positions[position_id]

            # 2. 주문 ID 수집
            order_ids = []

            # SL 주문
            sl_order_id = position.get("sl_order_id")
            if sl_order_id:
                order_ids.append(sl_order_id)

            # TP 주문
            tp_order_ids = position.get("tp_order_ids", [])
            order_ids.extend(tp_order_ids)

            # 활성 주문인지 확인
            active_order_ids = [order_id for order_id in order_ids if order_id in self.open_orders]

            # 3. 주문 취소
            cancelled_orders = []

            for order_id in active_order_ids:
                try:
                    cancel_result = await self.exchange.cancel_order(order_id)
                    cancelled_orders.append(cancel_result)

                    # 미체결 주문에서 제거
                    if order_id in self.open_orders:
                        del self.open_orders[order_id]

                    self.logger.info(f"주문 취소됨: {order_id}")

                except Exception as e:
                    self.logger.warning(f"주문 취소 실패: {order_id}, 오류: {e}")

            # 결과 업데이트
            result.update({
                "success": True,
                "cancelled_orders": cancelled_orders
            })

            self.logger.info(f"포지션 주문 취소 완료: {len(cancelled_orders)}개 취소됨")

        except Exception as e:
            error_msg = f"포지션 주문 취소 실패: {str(e)}"
            self.logger.error(error_msg)
            result["error"] = error_msg

        return result

    async def get_position_size(self, position_id: str) -> float:
        """
        포지션의 현재 크기를 가져옵니다.

        Args:
            position_id: 포지션 ID

        Returns:
            float: 포지션 크기 (코인 수량)
        """
        try:
            # 내부 포지션 정보 확인
            if position_id in self.active_positions:
                position = self.active_positions[position_id]

                # 거래소에서 최신 포지션 정보 가져오기
                if self.exchange.has['fetchPositions']:
                    positions = await self.exchange.fetch_positions([self.symbol]) or []

                    for pos in positions:
                        if pos['symbol'] == self.symbol:
                            # 방향 확인
                            stored_direction = position.get("direction", "long")
                            pos_side = pos.get('side', '').lower()

                            if (stored_direction == "long" and pos_side == "long") or \
                                    (stored_direction == "short" and pos_side == "short"):
                                # 포지션 크기 업데이트
                                size = float(pos.get('contracts', 0))

                                # 내부 정보 업데이트
                                if position.get("size") != size:
                                    self.active_positions[position_id]["size"] = size

                                return size

                # 거래소 조회 실패 시 저장된 값 반환
                return float(position.get("size", 0))

            return 0.0

        except Exception as e:
            self.logger.error(f"포지션 크기 조회 실패: {e}")
            return 0.0

    async def get_current_price(self) -> float:
        """
        현재 가격을 가져옵니다.

        Returns:
            float: 현재 가격
        """
        try:
            # 가장 최근 거래 가격 조회
            ticker = await self.exchange.fetch_ticker(self.symbol)
            return ticker['last'] or ticker['close'] or ticker['bid']

        except Exception as e:
            self.logger.error(f"현재 가격 조회 실패: {e}")

            try:
                # 대체 방법: 주문북에서 중간 가격
                orderbook = await self.exchange.fetch_order_book(self.symbol)
                bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else 0
                ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else 0

                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
                return bid or ask or 0

            except Exception as e2:
                self.logger.error(f"대체 가격 조회도 실패: {e2}")
                return 0

    async def save_position_to_redis(self, position_id: str) -> None:
        """
        포지션 정보를 Redis에 저장합니다.

        Args:
            position_id: 포지션 ID
        """
        try:
            if position_id in self.active_positions:
                position = self.active_positions[position_id]

                # Redis 키
                redis_key = f"position:{self.symbol.replace('/', '_')}:{position_id}"

                # 포지션 데이터 저장
                await asyncio.to_thread(
                    self.redis_client.set,
                    redis_key,
                    json.dumps(position)
                )

                # TTL 설정 (7일)
                await asyncio.to_thread(
                    self.redis_client.expire,
                    redis_key,
                    604800
                )

                # 스트림에도 추가
                stream_key = f"stream:positions:{self.symbol.replace('/', '_')}"
                await asyncio.to_thread(
                    self.redis_client.xadd,
                    stream_key,
                    {'data': json.dumps(position)},
                    maxlen=1000,
                    approximate=True
                )

                self.logger.debug(f"포지션 Redis에 저장됨: {position_id}")

        except Exception as e:
            self.logger.error(f"Redis에 포지션 저장 실패: {e}")

    async def record_position_in_db(
            self,
            position_id: str,
            action: str
    ) -> None:
        """
        포지션 정보를 데이터베이스에 기록합니다.

        Args:
            position_id: 포지션 ID
            action: 액션 유형 ('create', 'update', 'close')
        """
        try:
            if position_id in self.active_positions:
                position = self.active_positions[position_id]

                cursor = self.db_conn.cursor()

                if action == "create":
                    # 새 포지션 생성
                    cursor.execute(
                        """
                        INSERT INTO positions
                        (position_id, symbol, direction, size, entry_price, leverage,
                         entry_time, status, entry_order_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (position_id) 
                        DO UPDATE SET
                            size = EXCLUDED.size,
                            entry_price = EXCLUDED.entry_price,
                            leverage = EXCLUDED.leverage,
                            status = EXCLUDED.status,
                            last_update_time = CURRENT_TIMESTAMP
                        """,
                        (
                            position_id,
                            position.get("symbol"),
                            position.get("direction"),
                            position.get("size"),
                            position.get("entry_price"),
                            position.get("leverage"),
                            position.get("entry_time"),
                            position.get("status"),
                            position.get("entry_order_id")
                        )
                    )

                elif action == "update":
                    # 포지션 업데이트
                    cursor.execute(
                        """
                        UPDATE positions
                        SET size = %s,
                            unrealized_pnl = %s,
                            realized_pnl = %s,
                            sl_order_id = %s,
                            last_update_time = CURRENT_TIMESTAMP
                        WHERE position_id = %s
                        """,
                        (
                            position.get("size"),
                            position.get("unrealized_pnl"),
                            position.get("realized_pnl"),
                            position.get("sl_order_id"),
                            position_id
                        )
                    )

                elif action == "close":
                    # 포지션 종료
                    cursor.execute(
                        """
                        UPDATE positions
                        SET status = 'closed',
                            exit_price = %s,
                            exit_time = %s,
                            realized_pnl = %s,
                            last_update_time = CURRENT_TIMESTAMP
                        WHERE position_id = %s
                        """,
                        (
                            position.get("exit_price"),
                            position.get("exit_time"),
                            position.get("realized_pnl"),
                            position_id
                        )
                    )

                self.db_conn.commit()
                cursor.close()

                self.logger.debug(f"포지션 DB 기록 완료: {position_id}, 액션: {action}")

        except Exception as e:
            self.logger.error(f"DB에 포지션 기록 실패: {e}")
            if 'cursor' in locals():
                cursor.close()
                self.db_conn.rollback()

    async def update_position_pnl(self, position_id: str) -> Dict[str, Any]:
        """
        포지션의 손익을 업데이트합니다.

        Args:
            position_id: 포지션 ID

        Returns:
            Dict: 업데이트된 손익 정보
        """
        result = {
            "position_id": position_id,
            "unrealized_pnl": 0,
            "pnl_percent": 0,
            "updated": False
        }

        try:
            if position_id not in self.active_positions:
                return result

            position = self.active_positions[position_id]

            if position.get("status") == "closed":
                return result

            # 현재 가격 조회
            current_price = await self.get_current_price()

            if current_price <= 0:
                return result

            # 포지션 정보 가져오기
            direction = position.get("direction", "long")
            entry_price = position.get("entry_price", 0)
            size = position.get("size", 0)

            # 미실현 손익 계산
            if direction == "long":
                unrealized_pnl = (current_price - entry_price) * size
            else:
                unrealized_pnl = (entry_price - current_price) * size

            # 손익률 계산
            if entry_price > 0:
                if direction == "long":
                    pnl_percent = (current_price - entry_price) / entry_price * 100
                else:
                    pnl_percent = (entry_price - current_price) / entry_price * 100
            else:
                pnl_percent = 0

            # 포지션 업데이트
            position["unrealized_pnl"] = unrealized_pnl
            position["current_price"] = current_price
            position["last_update_time"] = int(time.time() * 1000)

            result.update({
                "unrealized_pnl": unrealized_pnl,
                "pnl_percent": pnl_percent,
                "current_price": current_price,
                "updated": True
            })

            # 초고수익 감지 및 자동 일부 청산 (설정된 경우)
            if self.config["auto_reduce_highly_profitable"] and \
                    pnl_percent > self.config["high_profit_threshold"]:
                await self.auto_reduce_profitable_position(position_id, pnl_percent)

        except Exception as e:
            self.logger.error(f"포지션 손익 업데이트 실패: {e}")

        return result

    async def auto_reduce_profitable_position(
            self,
            position_id: str,
            pnl_percent: float
    ) -> None:
        """
        초고수익 포지션을 자동으로 일부 청산합니다.

        Args:
            position_id: 포지션 ID
            pnl_percent: 수익률 (%)
        """
        try:
            position = self.active_positions[position_id]

            # 이미 Auto reduce 타임스탬프가 있는지 확인
            last_auto_reduce = position.get("last_auto_reduce", 0)
            current_time = int(time.time() * 1000)

            # 4시간(14400000ms)마다 한 번만 실행
            if current_time - last_auto_reduce < 14400000:
                return

            # 현재 크기와 청산할 크기 계산
            current_size = await self.get_position_size(position_id)
            reduce_pct = self.config["high_profit_reduce_percent"] / 100
            reduce_size = current_size * reduce_pct

            self.logger.info(
                f"초고수익 포지션 자동 일부 청산: "
                f"ID={position_id}, "
                f"수익률={pnl_percent:.2f}%, "
                f"청산비율={reduce_pct * 100}%, "
                f"청산량={reduce_size}"
            )

            # 포지션 일부 청산
            close_result = await self.close_position(position_id, reduce_size)

            if close_result["success"]:
                # 타임스탬프 업데이트
                position["last_auto_reduce"] = current_time

                self.logger.info(
                    f"초고수익 자동 일부 청산 완료: "
                    f"ID={position_id}, "
                    f"청산량={close_result['amount_closed']}, "
                    f"가격={close_result['close_price']}, "
                    f"실현손익=${close_result['pnl']:.2f}"
                )

        except Exception as e:
            self.logger.error(f"초고수익 포지션 자동 일부 청산 실패: {e}")

    async def run_position_monitor(self, interval: float = 60) -> None:
        """
        포지션 모니터링 루프를 실행합니다.

        Args:
            interval: 업데이트 간격 (초)
        """
        self.logger.info(f"포지션 모니터 시작, 간격: {interval}초")

        while True:
            try:
                # 활성 포지션 목록
                position_ids = list(self.active_positions.keys())

                for position_id in position_ids:
                    position = self.active_positions.get(position_id)

                    if position and position.get("status") != "closed":
                        # 손익 업데이트
                        await self.update_position_pnl(position_id)

                        # 포지션 크기 업데이트
                        size = await self.get_position_size(position_id)

                        if size <= 0:
                            # 포지션이 외부에서 종료됨
                            self.logger.info(f"포지션이 외부에서 종료됨: {position_id}")
                            position["status"] = "closed"
                            position["exit_time"] = int(time.time() * 1000)

                            # 데이터베이스에 기록
                            await self.record_position_in_db(position_id, "close")

                # Redis 동기화
                for position_id in position_ids:
                    await self.save_position_to_redis(position_id)

                # 주문 업데이트 (필요시)
                if len(self.open_orders) > 0:
                    await self.update_open_orders()

            except Exception as e:
                self.logger.error(f"포지션 모니터링 중 오류 발생: {e}")

            await asyncio.sleep(interval)

    async def update_open_orders(self) -> None:
        """미체결 주문 상태를 업데이트합니다."""
        try:
            # 현재 미체결 주문 조회
            latest_orders = await self.exchange.fetch_open_orders(self.symbol)

            # 최신 주문 ID 집합
            latest_order_ids = {order['id'] for order in latest_orders if 'id' in order}

            # 내부 캐시와 비교하여 체결/취소된 주문 찾기
            closed_order_ids = []

            for order_id in self.open_orders:
                if order_id not in latest_order_ids:
                    closed_order_ids.append(order_id)

            # 체결/취소된 주문 제거
            for order_id in closed_order_ids:
                # 각 포지션의 TP/SL 필드 업데이트 (필요시)
                for position_id, position in self.active_positions.items():
                    # SL 주문 확인
                    if position.get("sl_order_id") == order_id:
                        self.logger.info(f"SL 주문 체결/취소: ID={order_id}, 포지션={position_id}")
                        position["sl_order_id"] = None

                    # TP 주문 확인
                    if order_id in position.get("tp_order_ids", []):
                        self.logger.info(f"TP 주문 체결/취소: ID={order_id}, 포지션={position_id}")
                        position["tp_order_ids"] = [id for id in position.get("tp_order_ids", []) if id != order_id]

                # 미체결 주문에서 제거
                del self.open_orders[order_id]

            # 새 주문 추가
            for order in latest_orders:
                order_id = order.get('id')
                if order_id and order_id not in self.open_orders:
                    self.open_orders[order_id] = order

            self.logger.debug(f"미체결 주문 업데이트 완료: {len(self.open_orders)}개")

        except Exception as e:
            self.logger.error(f"미체결 주문 업데이트 실패: {e}")

    async def get_position_history(
            self,
            start_time: Optional[int] = None,
            end_time: Optional[int] = None,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        포지션 거래 내역을 가져옵니다.

        Args:
            start_time: 시작 시간 (밀리초 타임스탬프)
            end_time: 종료 시간 (밀리초 타임스탬프)
            limit: 최대 결과 수

        Returns:
            List[Dict]: 포지션 내역 목록
        """
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT 
                    position_id, symbol, direction, size, entry_price, leverage,
                    exit_price, entry_time, exit_time, status, realized_pnl
                FROM 
                    positions
                WHERE 
                    symbol = %s
            """

            params = [self.symbol]

            if start_time:
                query += " AND entry_time >= %s"
                params.append(start_time)

            if end_time:
                query += " AND (exit_time <= %s OR (exit_time IS NULL AND entry_time <= %s))"
                params.append(end_time)
                params.append(end_time)

            query += " ORDER BY entry_time DESC LIMIT %s"
            params.append(limit)

            cursor.execute(query, params)

            positions = []
            for row in cursor.fetchall():
                (position_id, symbol, direction, size, entry_price, leverage,
                 exit_price, entry_time, exit_time, status, realized_pnl) = row

                positions.append({
                    "position_id": position_id,
                    "symbol": symbol,
                    "direction": direction,
                    "size": float(size) if size else 0,
                    "entry_price": float(entry_price) if entry_price else 0,
                    "leverage": float(leverage) if leverage else 1,
                    "exit_price": float(exit_price) if exit_price else 0,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "status": status,
                    "realized_pnl": float(realized_pnl) if realized_pnl else 0
                })

            return positions

        except Exception as e:
            self.logger.error(f"포지션 내역 조회 실패: {e}")
            return []
        finally:
            if 'cursor' in locals():
                cursor.close()

    async def get_wallet_balance(self) -> Dict[str, Any]:
        """
        지갑 잔고를 조회합니다.

        Returns:
            Dict: 잔고 정보
        """
        try:
            # 잔고 조회
            balance = await self.exchange.fetch_balance()

            # 결과 구성
            result = {
                "total": {},
                "free": {},
                "used": {},
                "timestamp": int(time.time() * 1000)
            }

            # 심볼에서 베이스 및 쿼트 통화 추출
            symbol_parts = self.symbol.split('/')
            base_currency = symbol_parts[0] if len(symbol_parts) > 0 else None
            quote_currency = symbol_parts[1] if len(symbol_parts) > 1 else None

            # 관련 통화 잔고 추출
            currencies = [base_currency, quote_currency, 'USDT', 'USD', 'BTC']

            for currency in currencies:
                if currency and currency in balance:
                    currency_balance = balance[currency]

                    result["total"][currency] = currency_balance.get("total", 0)
                    result["free"][currency] = currency_balance.get("free", 0)
                    result["used"][currency] = currency_balance.get("used", 0)

            return result

        except Exception as e:
            self.logger.error(f"지갑 잔고 조회 실패: {e}")
            return {"error": str(e)}

    async def close(self) -> None:
        """
        자원을 정리합니다.
        """
        self.logger.info("포지션 관리자 종료 중...")

        try:
            # 활성 작업 취소
            for task_name, task in self.active_tasks.items():
                if not task.done():
                    task.cancel()
                    self.logger.info(f"작업 취소됨: {task_name}")

            # Redis 연결 종료
            if hasattr(self, 'redis_client') and self.redis_client:
                await asyncio.to_thread(self.redis_client.close)
                self.logger.info("Redis 연결 종료됨")

            # PostgreSQL 연결 종료
            if hasattr(self, 'db_conn') and self.db_conn:
                await asyncio.to_thread(self.db_conn.close)
                self.logger.info("PostgreSQL 연결 종료됨")

            # 거래소 연결 종료
            if hasattr(self, 'exchange') and self.exchange:
                await self.exchange.close()
                self.logger.info(f"{self.exchange_id} 거래소 연결 종료됨")

        except Exception as e:
            self.logger.error(f"포지션 관리자 종료 중 오류 발생: {e}")


# 직접 실행 시 포지션 관리자 시작
if __name__ == "__main__":
    async def main():
        # 포지션 관리자 초기화
        position_manager = PositionManager("BTC/USDT", "binance")

        try:
            # 초기화
            await position_manager.initialize()

            # 지갑 잔고 확인
            balance = await position_manager.get_wallet_balance()
            print(f"지갑 잔고: {balance}")

            # 모니터링 작업 시작 (배경에서 실행)
            monitor_task = asyncio.create_task(position_manager.run_position_monitor())
            position_manager.active_tasks["monitor"] = monitor_task

            # 간단한 데모 포지션 생성 (시뮬레이션 모드)
            position_manager.config["simulated_mode"] = True

            entry_params = {
                "entry_price": 30000.0,
                "stop_loss": 29000.0,
                "take_profit_levels": [32000.0, 34000.0, 36000.0],
                "tp_percentages": [0.3, 0.3, 0.4],
                "leverage": 5
            }

            result = await position_manager.create_position("long", 0.1, entry_params)
            print(f"\n포지션 생성 결과: ID={result.get('position_id')}, 성공={result.get('success')}")

            # 10초 대기
            print("\n10초 동안 모니터링...")
            await asyncio.sleep(10)

            # 포지션 정보 표시
            position_id = result.get('position_id')
            if position_id:
                pnl_info = await position_manager.update_position_pnl(position_id)
                print(f"\n미실현 손익: ${pnl_info.get('unrealized_pnl'):.2f} ({pnl_info.get('pnl_percent'):.2f}%)")

                # 포지션 청산
                close_result = await position_manager.close_position(position_id)
                print(f"\n포지션 청산 결과: 성공={close_result.get('success')}, 손익=${close_result.get('pnl'):.2f}")

        except Exception as e:
            print(f"오류 발생: {e}")

        finally:
            # 정리
            await position_manager.close()


    # 이벤트 루프 실행
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("사용자에 의해 중단됨")
