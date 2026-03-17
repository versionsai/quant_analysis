# -*- coding: utf-8 -*-
"""
动量选股策略
基于历史涨幅进行选股
"""
from typing import Dict, List, Optional
import pandas as pd
from dataclasses import dataclass

from strategy.selectors.base_selector import BaseSelector, SelectResult, StockScore


class MomentumSelector(BaseSelector):
    """
    动量选股策略
    
    选股逻辑:
    1. 计算N日内涨幅
    2. 涨幅排名前N的股票入选
    3. 结合成交量过滤假突破
    """
    
    def __init__(
        self,
        period: int = 20,
        volume_filter: bool = True,
        min_volume_ratio: float = 0.5,
    ):
        super().__init__("Momentum")
        self.period = period
        self.volume_filter = volume_filter
        self.min_volume_ratio = min_volume_ratio
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """动量选股"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.period:
                continue
            
            returns = self._calc_momentum(df)
            if pd.isna(returns):
                continue
            
            if self.volume_filter:
                vol_ratio = self.calc_volume_ratio(df)
                if vol_ratio < self.min_volume_ratio:
                    continue
            
            reason = f"近{self.period}日涨幅: {returns:.2%}"
            scores.append(StockScore(
                symbol=symbol,
                score=returns,
                reason=reason
            ))
        
        scores.sort(key=lambda x: x.score, reverse=True)
        
        for i, s in enumerate(scores):
            s.rank = i + 1
        
        return SelectResult(
            stocks=scores[:top_n],
            pool_size=len(symbols),
            selected_count=len(scores[:top_n])
        )
    
    def _calc_momentum(self, df: pd.DataFrame) -> float:
        """计算动量因子"""
        if "close" not in df.columns:
            return 0.0
        
        current_price = df["close"].iloc[-1]
        past_price = df["close"].iloc[-self.period]
        
        if past_price == 0:
            return 0.0
        
        return (current_price / past_price) - 1


class DualMomentumSelector(BaseSelector):
    """
    双重动量选股策略
    
    同时考虑:
    1. 相对动量 (相对于自身历史)
    2. 绝对动量 (相对于市场/基准)
    """
    
    def __init__(
        self,
        short_period: int = 20,
        long_period: int = 60,
        lookback: str = "short",
    ):
        super().__init__("DualMomentum")
        self.short_period = short_period
        self.long_period = long_period
        self.lookback = lookback
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """双重动量选股"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.long_period:
                continue
            
            short_ret = self._calc_return(df, self.short_period)
            long_ret = self._calc_return(df, self.long_period)
            
            if pd.isna(short_ret) or pd.isna(long_ret):
                continue
            
            if self.lookback == "short":
                score = short_ret
            elif self.lookback == "long":
                score = long_ret
            else:
                score = (short_ret + long_ret) / 2
            
            reason = f"短期:{short_ret:.2%} 长期:{long_ret:.2%}"
            scores.append(StockScore(
                symbol=symbol,
                score=score,
                reason=reason
            ))
        
        scores.sort(key=lambda x: x.score, reverse=True)
        
        for i, s in enumerate(scores):
            s.rank = i + 1
        
        return SelectResult(
            stocks=scores[:top_n],
            pool_size=len(symbols),
            selected_count=len(scores[:top_n])
        )
    
    def _calc_return(self, df: pd.DataFrame, period: int) -> float:
        """计算区间收益率"""
        if "close" not in df.columns:
            return 0.0
        
        current = df["close"].iloc[-1]
        past = df["close"].iloc[-period]
        
        if past == 0:
            return 0.0
        
        return (current / past) - 1


class RotationSelector(BaseSelector):
    """
    行业/主题轮动选股
    
    动量效应的行业轮动:
    - 买入近期涨幅靠前的行业
    - 卖出近期涨幅靠后的行业
    """
    
    def __init__(
        self,
        period: int = 20,
        top_n: int = 5,
        bottom_n: int = 5,
    ):
        super().__init__("Rotation")
        self.period = period
        self.top_n = top_n
        self.bottom_n = bottom_n
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """行业轮动选股"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.period:
                continue
            
            returns = self._calc_return(df)
            if pd.isna(returns):
                continue
            
            scores.append(StockScore(
                symbol=symbol,
                score=returns,
                reason=f"近{self.period}日涨幅: {returns:.2%}"
            ))
        
        scores.sort(key=lambda x: x.score, reverse=True)
        
        selected = scores[:self.top_n]
        selected.extend(scores[-self.bottom_n:] if len(scores) >= self.bottom_n else [])
        
        for i, s in enumerate(selected):
            s.rank = i + 1
        
        return SelectResult(
            stocks=selected[:top_n],
            pool_size=len(symbols),
            selected_count=len(selected[:top_n])
        )
    
    def _calc_return(self, df: pd.DataFrame) -> float:
        """计算收益率"""
        if "close" not in df.columns:
            return 0.0
        
        current = df["close"].iloc[-1]
        past = df["close"].iloc[-self.period]
        
        if past == 0:
            return 0.0
        
        return (current / past) - 1
