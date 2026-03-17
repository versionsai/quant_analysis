# -*- coding: utf-8 -*-
"""
订单管理
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Order:
    """订单"""
    order_id: str
    symbol: str
    direction: str  # buy/sell
    order_type: str  # limit/market
    price: float
    quantity: int
    filled_quantity: int = 0
    status: str = "pending"  # pending/filled/canceled/rejected
    create_time: datetime = None
    
    def __post_init__(self):
        if self.create_time is None:
            self.create_time = datetime.now()


@dataclass
class TradeRecord:
    """成交记录"""
    trade_id: str
    order_id: str
    symbol: str
    direction: str
    price: float
    quantity: int
    trade_time: datetime


class OrderManager:
    """订单管理器"""
    
    def __init__(self):
        self.orders: dict = {}
        self.trades: list = []
        self.order_counter = 0
    
    def create_order(self, symbol: str, direction: str, order_type: str,
                    price: float, quantity: int) -> Order:
        """创建订单"""
        self.order_counter += 1
        order_id = f"ORD{self.order_counter:08d}"
        
        order = Order(
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            quantity=quantity
        )
        
        self.orders[order_id] = order
        return order
    
    def fill_order(self, order_id: str, fill_price: float, fill_quantity: int):
        """成交订单"""
        if order_id not in self.orders:
            return
        
        order = self.orders[order_id]
        order.filled_quantity += fill_quantity
        
        if order.filled_quantity >= order.quantity:
            order.status = "filled"
        
        trade = TradeRecord(
            trade_id=f"TRD{len(self.trades) + 1:08d}",
            order_id=order_id,
            symbol=order.symbol,
            direction=order.direction,
            price=fill_price,
            quantity=fill_quantity,
            trade_time=datetime.now()
        )
        self.trades.append(trade)
    
    def cancel_order(self, order_id: str):
        """取消订单"""
        if order_id in self.orders:
            self.orders[order_id].status = "canceled"
    
    def get_pending_orders(self) -> list:
        """获取待成交订单"""
        return [o for o in self.orders.values() if o.status == "pending"]
