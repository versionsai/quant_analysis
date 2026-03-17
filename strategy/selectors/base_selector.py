# -*- coding: utf-8 -*-
"""
选股策略基类
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class StockScore:
    """股票评分"""
    symbol: str
    score: float
    rank: int = 0
    reason: str = ""


@dataclass
class SelectResult:
    """选股结果"""
    stocks: List[StockScore]
    pool_size: int
    selected_count: int
    
    def get_symbols(self, top_n: Optional[int] = None) -> List[str]:
        if top_n is None:
            return [s.symbol for s in self.stocks]
        return [s.symbol for s in self.stocks[:top_n]]


class BaseSelector(ABC):
    """选股策略基类"""
    
    def __init__(self, name: str = "base_selector"):
        self.name = name
        self.data: Dict[str, pd.DataFrame] = {}
    
    @abstractmethod
    def select(
        self, 
        symbols: List[str], 
        start_date: str, 
        end_date: str,
        top_n: int = 10
    ) -> SelectResult:
        """
        从股票池中选股
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            top_n: 返回前N只股票
        
        Returns:
            SelectResult: 选股结果
        """
        pass
    
    def load_data(self, symbol: str, df: pd.DataFrame):
        """加载数据"""
        self.data[symbol] = df
    
    def get_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """获取数据"""
        return self.data.get(symbol)
    
    def calc_returns(self, df: pd.DataFrame, periods: List[int] = [5, 10, 20]) -> pd.Series:
        """计算各周期收益率"""
        if df is None or df.empty or "close" not in df.columns:
            return pd.Series(dtype=float)
        
        returns = {}
        for p in periods:
            returns[f"return_{p}d"] = (df["close"] / df["close"].shift(p) - 1).iloc[-1]
        return pd.Series(returns)
    
    def calc_volume_ratio(self, df: pd.DataFrame, period: int = 20) -> float:
        """计算成交量比"""
        if df is None or df.empty or "volume" not in df.columns:
            return 0.0
        return df["volume"].iloc[-1] / df["volume"].rolling(period).mean().iloc[-1]


class MultiFactorSelector(BaseSelector):
    """多因子选股基类"""
    
    def __init__(self, name: str = "multi_factor"):
        super().__init__(name)
        self.factors: Dict[str, callable] = {}
        self.weights: Dict[str, float] = {}
    
    def add_factor(self, name: str, func: callable, weight: float = 1.0):
        """添加因子"""
        self.factors[name] = func
        self.weights[name] = weight
    
    def calc_factor_values(self, df: pd.DataFrame) -> pd.Series:
        """计算因子值"""
        if df is None or df.empty:
            return pd.Series(dtype=float)
        
        values = {}
        for name, func in self.factors.items():
            try:
                values[name] = func(df)
            except Exception:
                values[name] = 0.0
        return pd.Series(values)
    
    def normalize(self, series: pd.Series) -> pd.Series:
        """标准化因子值 (z-score)"""
        if series.std() == 0:
            return pd.Series(0, index=series.index)
        return (series - series.mean()) / series.std()
