# -*- coding: utf-8 -*-
"""
持仓追踪器

追踪持仓变动、成本计算、持仓时长等
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, date
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    name: str
    shares: int
    avg_cost: float
    first_buy_date: str = ""
    
    current_price: float = 0.0
    current_value: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    
    hold_days: int = 0
    last_update: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    buy_history: List[Dict] = field(default_factory=list)
    sell_history: List[Dict] = field(default_factory=list)
    
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    
    def update_price(self, price: float):
        self.current_price = price
        self.current_value = self.shares * price
        self.unrealized_pnl = self.current_value - (self.shares * self.avg_cost)
        self.unrealized_pnl_pct = (price / self.avg_cost - 1) * 100 if self.avg_cost > 0 else 0
        self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "shares": self.shares,
            "avg_cost": self.avg_cost,
            "current_price": self.current_price,
            "current_value": self.current_value,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "hold_days": self.hold_days,
            "tags": self.tags,
            "notes": self.notes,
        }


class PortfolioTracker:
    """持仓追踪器"""
    
    def __init__(self, initial_capital: float = 1000000):
        self.initial_capital = initial_capital
        self.cash: float = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trade_history: List[Dict] = []
        self.daily_value: List[Dict] = []
    
    @property
    def total_value(self) -> float:
        return self.cash + sum(p.current_value for p in self.positions.values())
    
    @property
    def total_cost(self) -> float:
        return sum(p.shares * p.avg_cost for p in self.positions.values())
    
    @property
    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())
    
    @property
    def total_return(self) -> float:
        return (self.total_value / self.initial_capital - 1) * 100 if self.initial_capital > 0 else 0
    
    def add_position(
        self,
        symbol: str,
        name: str,
        shares: int,
        price: float,
        trade_date: str = None,
    ):
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        
        if symbol in self.positions:
            pos = self.positions[symbol]
            total_cost = pos.shares * pos.avg_cost + shares * price
            pos.shares += shares
            pos.avg_cost = total_cost / pos.shares
            pos.update_price(price)
            pos.buy_history.append({
                "date": trade_date,
                "shares": shares,
                "price": price,
                "action": "加仓",
            })
        else:
            pos = Position(
                symbol=symbol,
                name=name,
                shares=shares,
                avg_cost=price,
                first_buy_date=trade_date,
            )
            pos.update_price(price)
            pos.buy_history.append({
                "date": trade_date,
                "shares": shares,
                "price": price,
                "action": "买入",
            })
            self.positions[symbol] = pos
        
        self.cash -= shares * price
        self._record_trade(symbol, name, "买入", shares, price, trade_date)
        logger.info(f"买入 {symbol} {name}: {shares}股 @{price:.2f}")
    
    def reduce_position(
        self,
        symbol: str,
        shares: int,
        price: float,
        trade_date: str = None,
    ):
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        
        if symbol not in self.positions:
            logger.warning(f"尝试卖出未持有的股票: {symbol}")
            return False
        
        pos = self.positions[symbol]
        if shares > pos.shares:
            shares = pos.shares
        
        realized_pnl = (price - pos.avg_cost) * shares
        pos.shares -= shares
        pos.current_price = price
        self.cash += shares * price
        
        pos.sell_history.append({
            "date": trade_date,
            "shares": shares,
            "price": price,
            "realized_pnl": realized_pnl,
            "action": "减仓" if pos.shares > 0 else "清仓",
        })
        
        self._record_trade(symbol, pos.name, "卖出", shares, price, trade_date)
        
        if pos.shares == 0:
            del self.positions[symbol]
            logger.info(f"清仓 {symbol}: 盈利{realized_pnl:.2f}")
        else:
            pos.update_price(price)
            logger.info(f"减仓 {symbol}: {shares}股 @{price:.2f}")
        
        return True
    
    def update_prices(self, prices: Dict[str, float]):
        for symbol, price in prices.items():
            if symbol in self.positions:
                self.positions[symbol].update_price(price)
    
    def get_position(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)
    
    def get_positions_by_tag(self, tag: str) -> List[Position]:
        return [p for p in self.positions.values() if tag in p.tags]
    
    def add_tag(self, symbol: str, tag: str):
        if symbol in self.positions:
            if tag not in self.positions[symbol].tags:
                self.positions[symbol].tags.append(tag)
    
    def remove_tag(self, symbol: str, tag: str):
        if symbol in self.positions:
            if tag in self.positions[symbol].tags:
                self.positions[symbol].tags.remove(tag)
    
    def _record_trade(
        self,
        symbol: str,
        name: str,
        action: str,
        shares: int,
        price: float,
        trade_date: str,
    ):
        self.trade_history.append({
            "date": trade_date,
            "symbol": symbol,
            "name": name,
            "action": action,
            "shares": shares,
            "price": price,
            "amount": shares * price,
            "timestamp": datetime.now().isoformat(),
        })
    
    def record_daily_value(self, date_str: str = None, index_value: float = None):
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        
        self.daily_value.append({
            "date": date_str,
            "total_value": self.total_value,
            "cash": self.cash,
            "position_value": self.total_value - self.cash,
            "unrealized_pnl": self.total_unrealized_pnl,
            "total_return": self.total_return,
            "index_value": index_value,
        })
    
    def to_dict(self) -> dict:
        return {
            "initial_capital": self.initial_capital,
            "cash": self.cash,
            "total_value": self.total_value,
            "total_return": self.total_return,
            "positions": {symbol: pos.to_dict() for symbol, pos in self.positions.items()},
            "trade_count": len(self.trade_history),
        }
    
    def summary(self) -> str:
        lines = [
            f"【持仓追踪】",
            f"总资产: {self.total_value:,.2f}",
            f"持仓市值: {self.total_value - self.cash:,.2f}",
            f"现金: {self.cash:,.2f}",
            f"浮盈亏: {self.total_unrealized_pnl:,.2f} ({self.total_return:.2f}%)",
            f"持仓数: {len(self.positions)}",
        ]
        if self.positions:
            lines.append("持仓明细:")
            for pos in sorted(self.positions.values(), key=lambda x: -x.unrealized_pnl):
                lines.append(
                    f"  {pos.symbol} {pos.name}: "
                    f"{pos.shares}股 @{pos.avg_cost:.2f} → {pos.current_price:.2f} "
                    f"{pos.unrealized_pnl:+.2f}({pos.unrealized_pnl_pct:+.2f}%)"
                )
        return "\n".join(lines)
