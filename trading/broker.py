# -*- coding: utf-8 -*-
"""
券商接口 (模拟)
"""
from typing import Dict, List, Optional
from .orders import OrderManager, Order
from utils.logger import get_logger

logger = get_logger(__name__)


class Broker:
    """券商接口基类"""
    
    def __init__(self, name: str = "sim"):
        self.name = name
        self.order_manager = OrderManager()
        self.positions: Dict[str, dict] = {}
        self.balance: float = 0.0
    
    def connect(self) -> bool:
        """连接券商"""
        logger.info(f"连接券商: {self.name}")
        return True
    
    def disconnect(self):
        """断开连接"""
        logger.info(f"断开券商: {self.name}")
    
    def get_account(self) -> dict:
        """获取账户信息"""
        return {
            "balance": self.balance,
            "positions": self.positions,
        }
    
    def get_positions(self) -> Dict[str, dict]:
        """获取持仓"""
        return self.positions
    
    def get_realtime_quote(self, symbol: str) -> Optional[float]:
        """获取实时报价 (模拟)"""
        return None
    
    def place_order(self, symbol: str, direction: str, price: float, 
                    quantity: int, order_type: str = "limit") -> Order:
        """下单"""
        order = self.order_manager.create_order(
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            quantity=quantity
        )
        
        logger.info(f"下单: {symbol} {direction} {quantity}@{price}")
        return order
    
    def cancel_order(self, order_id: str):
        """撤单"""
        self.order_manager.cancel_order(order_id)
        logger.info(f"撤单: {order_id}")


class SimBroker(Broker):
    """模拟券商"""
    
    def __init__(self):
        super().__init__("sim")
        self.balance = 1000000
    
    def get_realtime_quote(self, symbol: str) -> Optional[float]:
        """获取实时报价"""
        return None
