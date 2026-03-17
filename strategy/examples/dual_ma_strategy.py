# -*- coding: utf-8 -*-
"""
示例策略 - 双均线策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime


class DualMAStrategy(BaseStrategy):
    """双均线策略"""
    
    def __init__(self, fast: int = 10, slow: int = 30):
        super().__init__("DualMA")
        self.fast = fast
        self.slow = slow
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.slow + 1:
            return None
        
        df = df.copy()
        df["fast_ma"] = df["close"].rolling(self.fast).mean()
        df["slow_ma"] = df["close"].rolling(self.slow).mean()
        df["signal"] = np.where(df["fast_ma"] > df["slow_ma"], 1, -1)
        df["prev_signal"] = df["signal"].shift(1)
        
        latest = df.iloc[-1]
        
        if latest["signal"] == 1 and latest["prev_signal"] == -1:
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        elif latest["signal"] == -1 and latest["prev_signal"] == 1:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
