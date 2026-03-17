# -*- coding: utf-8 -*-
"""
示例策略 - 简单移动平均策略
"""
import pandas as pd
from strategy.base import BaseStrategy, Signal
from datetime import datetime


class MAStrategy(BaseStrategy):
    """
    简单MA策略
    金叉买入，死叉卖出
    """
    
    def __init__(self, short_ma: int = 5, long_ma: int = 20):
        super().__init__("MA_Strategy")
        self.short_ma = short_ma
        self.long_ma = long_ma
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.long_ma + 1:
            return None
        
        df = df.copy()
        df["ma_short"] = df["close"].rolling(self.short_ma).mean()
        df["ma_long"] = df["close"].rolling(self.long_ma).mean()
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if pd.isna(latest["ma_short"]) or pd.isna(latest["ma_long"]):
            return None
        
        if prev["ma_short"] <= prev["ma_long"] and latest["ma_short"] > latest["ma_long"]:
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        elif prev["ma_short"] >= prev["ma_long"] and latest["ma_short"] < latest["ma_long"]:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
