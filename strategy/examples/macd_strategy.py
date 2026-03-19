# -*- coding: utf-8 -*-
"""
MACD 策略
基于MACD指标的交易策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime


class MACDStrategy(BaseStrategy):
    """
    MACD 策略
    
    主要信号:
    1. MACD金叉 (买入)
    2. MACD死叉 (卖出)
    3. MACD零轴上方运行 (多头)
    4. MACD底背离 (买入)
    5. MACD顶背离 (卖出)
    """
    
    def __init__(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        use_divergence: bool = True,
    ):
        super().__init__("MACD")
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.use_divergence = use_divergence
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.slow + 10:
            return None
        
        df = df.copy()
        df = self._calc_macd(df)
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if any(pd.isna([latest.get(k, 0) for k in ['macd', 'signal', 'histogram']])):
            return None
        
        if self._check_golden_cross(prev, latest):
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        
        if self._check_death_cross(prev, latest):
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        if self.use_divergence:
            if self._check_bottom_divergence(df):
                return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
            
            if self._check_top_divergence(df):
                return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
    
    def _calc_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算MACD指标"""
        exp1 = df["close"].ewm(span=self.fast, adjust=False).mean()
        exp2 = df["close"].ewm(span=self.slow, adjust=False).mean()
        
        df["macd"] = exp1 - exp2
        df["signal"] = df["macd"].ewm(span=self.signal, adjust=False).mean()
        df["histogram"] = df["macd"] - df["signal"]
        
        df["macd_above_zero"] = df["macd"] > 0
        df["signal_above_zero"] = df["signal"] > 0
        
        return df
    
    def _check_golden_cross(self, prev: pd.Series, latest: pd.Series) -> bool:
        """金叉: MACD从下方穿越信号线"""
        prev_cross = prev["macd"] <= prev["signal"]
        curr_cross = latest["macd"] > latest["signal"]
        return prev_cross and curr_cross
    
    def _check_death_cross(self, prev: pd.Series, latest: pd.Series) -> bool:
        """死叉: MACD从上方穿越信号线"""
        prev_cross = prev["macd"] >= prev["signal"]
        curr_cross = latest["macd"] < latest["signal"]
        return prev_cross and curr_cross
    
    def _check_bottom_divergence(self, df: pd.DataFrame) -> bool:
        """底背离: 价格创新低，MACD未创新低"""
        if len(df) < 34:
            return False
        
        prices = df["close"].values
        macd_vals = df["macd"].values
        
        recent_lows = []
        for i in range(-10, 0):
            if len(recent_lows) < 3:
                recent_lows.append((i, prices[i], macd_vals[i]))
        
        if len(recent_lows) >= 2:
            price_low_1 = min(prices[-15:-10])
            price_low_2 = min(prices[-10:-5])
            macd_low_1 = min(macd_vals[-15:-10])
            macd_low_2 = min(macd_vals[-10:-5])
            
            if price_low_2 < price_low_1 and macd_low_2 > macd_low_1:
                return True
        
        return False
    
    def _check_top_divergence(self, df: pd.DataFrame) -> bool:
        """顶背离: 价格创新高，MACD未创新高"""
        if len(df) < 34:
            return False
        
        prices = df["close"].values
        macd_vals = df["macd"].values
        
        price_high_1 = max(prices[-15:-10])
        price_high_2 = max(prices[-10:-5])
        macd_high_1 = max(macd_vals[-15:-10])
        macd_high_2 = max(macd_vals[-10:-5])
        
        if price_high_2 > price_high_1 and macd_high_2 < macd_high_1:
            return True
        
        return False


class MACDTrendStrategy(BaseStrategy):
    """
    MACD趋势策略
    结合零轴判断和交叉信号
    """
    
    def __init__(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ):
        super().__init__("MACDTrend")
        self.fast = fast
        self.slow = slow
        self.signal = signal
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.slow + 5:
            return None
        
        df = df.copy()
        
        exp1 = df["close"].ewm(span=self.fast, adjust=False).mean()
        exp2 = df["close"].ewm(span=self.slow, adjust=False).mean()
        df["macd"] = exp1 - exp2
        df["signal"] = df["macd"].ewm(span=self.signal, adjust=False).mean()
        df["histogram"] = df["macd"] - df["signal"]
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if latest["macd"] > 0 and latest["signal"] > 0:
            if prev["macd"] <= prev["signal"] and latest["macd"] > latest["signal"]:
                return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        
        if latest["macd"] < 0 or latest["signal"] < 0:
            if prev["macd"] >= prev["signal"] and latest["macd"] < latest["signal"]:
                return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)


class MACDRSIStrategy(BaseStrategy):
    """
    MACD + RSI 组合策略
    """
    
    def __init__(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        rsi_period: int = 14,
        rsi_oversold: float = 30,
        rsi_overbought: float = 70,
    ):
        super().__init__("MACD_RSI")
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.rsi_period = rsi_period
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < max(self.slow, self.rsi_period) + 5:
            return None
        
        df = df.copy()
        
        exp1 = df["close"].ewm(span=self.fast, adjust=False).mean()
        exp2 = df["close"].ewm(span=self.slow, adjust=False).mean()
        df["macd"] = exp1 - exp2
        df["signal"] = df["macd"].ewm(span=self.signal, adjust=False).mean()
        
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)
        avg_gain = gain.rolling(self.rsi_period).mean()
        avg_loss = loss.rolling(self.rsi_period).mean()
        rs = avg_gain / (avg_loss + 0.0001)
        df["rsi"] = 100 - (100 / (1 + rs))
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        macd_golden = prev["macd"] <= prev["signal"] and latest["macd"] > latest["signal"]
        rsi_buy = latest["rsi"] < self.rsi_oversold
        
        if macd_golden and rsi_buy:
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        
        macd_death = prev["macd"] >= prev["signal"] and latest["macd"] < latest["signal"]
        rsi_sell = latest["rsi"] > self.rsi_overbought
        
        if macd_death and rsi_sell:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
