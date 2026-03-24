# -*- coding: utf-8 -*-
"""
策略基类
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Signal:
    """交易信号"""
    symbol: str
    date: datetime
    signal: float  # 1: 买入, -1: 卖出, 0: 持有/观望
    weight: float = 0.0  # 仓位权重
    candidate_score: float = 0.0  # 候选信号评分（用于调优分析）
    gate_passed: bool = True  # 是否通过最终放行
    gate_reason: str = ""  # 放行/拦截原因


@dataclass
class Position:
    """持仓"""
    symbol: str
    quantity: int = 0
    cost: float = 0.0  # 成本价
    current_price: float = 0.0
    entry_date: Optional[datetime] = None  # 买入日期（用于T+1/时间止损）
    entry_low: float = 0.0  # 买入日低点（不及预期止损）
    highest_price: float = 0.0  # 持仓以来最高价（用于跟踪止盈）
    take_profit_stage: int = 0  # 分批止盈阶段：0未触发，1已触发第一档


@dataclass
class Portfolio:
    """组合状态"""
    cash: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    total_value: float = 0.0
    
    def get_position_value(self) -> float:
        return sum(p.quantity * p.current_price for p in self.positions.values())
    
    def get_total_value(self, cash: float = None) -> float:
        c = cash if cash is not None else self.cash
        return c + self.get_position_value()


class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str = "base"):
        self.name = name
        self.data: Dict[str, pd.DataFrame] = {}
        self.signals: List[Signal] = []
        self.market_context: Dict[str, object] = {}
    
    @abstractmethod
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        """
        逐K线回调
        子类实现具体策略逻辑
        """
        pass
    
    def on_start(self):
        """策略启动回调"""
        pass
    
    def on_end(self):
        """策略结束回调"""
        pass
    
    def load_data(self, symbol: str, df: pd.DataFrame):
        """加载数据"""
        self.data[symbol] = df

    def set_market_context(self, context: Optional[Dict[str, object]] = None):
        """设置市场上下文。"""
        self.market_context = dict(context or {})

    def get_market_context(self) -> Dict[str, object]:
        """获取市场上下文。"""
        return dict(self.market_context or {})
    
    def get_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取数据"""
        return self.data.get(symbol)
    
    def generate_signals(self, symbols: List[str], date: datetime) -> List[Signal]:
        """批量生成信号"""
        signals = []
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty:
                continue
            signal = self.on_bar(symbol, df)
            if signal:
                signal.date = date
                signals.append(signal)
        return signals


class MultiFactorStrategy(BaseStrategy):
    """多因子策略基类"""
    
    def __init__(self, name: str = "multi_factor"):
        super().__init__(name)
        self.factors = {}
    
    def add_factor(self, name: str, func):
        """添加因子"""
        self.factors[name] = func
    
    def calc_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算因子"""
        for name, func in self.factors.items():
            df[name] = func(df)
        return df
