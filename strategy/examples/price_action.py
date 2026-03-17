# -*- coding: utf-8 -*-
"""
Price Action 策略
基于价格行为的交易策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime


class PriceActionStrategy(BaseStrategy):
    """
    Price Action 策略
    
    主要信号:
    1. 突破支撑/阻力位
    2. Pin Bar (锤子线/上吊线)
    3. 趋势突破 (高点/低点抬高)
    4. 区间突破
    """
    
    def __init__(
        self,
        lookback: int = 20,
        atr_period: int = 14,
        atr_multiplier: float = 2.0,
        min_body_ratio: float = 0.5,
    ):
        super().__init__("PriceAction")
        self.lookback = lookback
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.min_body_ratio = min_body_ratio
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.lookback + 5:
            return None
        
        df = df.copy()
        df = self._calc_indicators(df)
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if any(pd.isna([latest.get(k, 0) for k in ['swing_high', 'swing_low', 'atr', 'ema20']])):
            return None
        
        signals = []
        
        if self._check_breakout(df):
            signals.append(1)
        elif self._check_pinbar(df):
            signals.append(1)
        elif self._check_trend_reversal(df):
            signals.append(-1)
        elif self._check_breakdown(df):
            signals.append(-1)
        
        if signals and any(signals):
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        elif -1 in signals:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
    
    def _calc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        df["high"] = df.get("high", df["close"])
        df["low"] = df.get("low", df["close"])
        
        df["swing_high"] = df["high"].rolling(self.lookback).max()
        df["swing_low"] = df["low"].rolling(self.lookback).min()
        
        df["ema20"] = df["close"].ewm(span=20).mean()
        df["ema50"] = df["close"].ewm(span=50).mean()
        
        high_low = df["high"] - df["low"]
        high_close = abs(df["high"] - df["close"].shift())
        low_close = abs(df["low"] - df["close"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = tr.rolling(self.atr_period).mean()
        
        df["body"] = abs(df["close"] - df["open"])
        df["body_ratio"] = df["body"] / (df["high"] - df["low"] + 0.001)
        
        df["higher_high"] = (df["high"] > df["high"].shift(1)) & (df["high"] > df["high"].shift(2))
        df["lower_low"] = (df["low"] < df["low"].shift(1)) & (df["low"] < df["low"].shift(2))
        
        return df
    
    def _check_breakout(self, df: pd.DataFrame) -> bool:
        """检查突破信号"""
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if latest["close"] > latest["swing_high"] and latest["close"] > latest["ema20"]:
            if latest["volume"] > df["volume"].rolling(10).mean().iloc[-1] * 0.8:
                return True
        return False
    
    def _check_breakdown(self, df: pd.DataFrame) -> bool:
        """检查跌破信号"""
        latest = df.iloc[-1]
        
        if latest["close"] < latest["swing_low"] and latest["close"] < latest["ema20"]:
            return True
        return False
    
    def _check_pinbar(self, df: pd.DataFrame) -> bool:
        """检查Pin Bar信号"""
        if len(df) < 3:
            return False
        
        latest = df.iloc[-1]
        
        upper_shadow = latest["high"] - max(latest["open"], latest["close"])
        lower_shadow = min(latest["open"], latest["close"]) - latest["low"]
        body = abs(latest["close"] - latest["open"])
        
        if body < latest["atr"] * 0.3:
            return False
        
        total_range = latest["high"] - latest["low"] + 0.001
        
        if lower_shadow > body * 2 and lower_shadow > total_range * 0.6:
            if latest["close"] > latest["ema20"]:
                return True
        
        if upper_shadow > body * 2 and upper_shadow > total_range * 0.6:
            if latest["close"] < latest["ema20"]:
                return True
        
        return False
    
    def _check_trend_reversal(self, df: pd.DataFrame) -> bool:
        """检查趋势反转"""
        if len(df) < 5:
            return False
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if latest["close"] < latest["ema20"] and latest["ema20"] < latest["ema50"]:
            return True
        
        return False


class BreakoutStrategy(BaseStrategy):
    """
    突破策略
    专注于区间突破和趋势确认
    """
    
    def __init__(self, lookback: int = 20, volume_ratio: float = 1.5):
        super().__init__("Breakout")
        self.lookback = lookback
        self.volume_ratio = volume_ratio
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.lookback + 2:
            return None
        
        df = df.copy()
        df["high_max"] = df["high"].rolling(self.lookback).max()
        df["low_min"] = df["low"].rolling(self.lookback).min()
        df["vol_ma"] = df["volume"].rolling(10).mean()
        df["ema20"] = df["close"].ewm(span=20).mean()
        
        latest = df.iloc[-1]
        
        resistance = latest["high_max"]
        support = latest["low_min"]
        price_range = resistance - support
        
        if price_range < latest["close"] * 0.02:
            return None
        
        if latest["close"] > resistance:
            if latest["volume"] > latest["vol_ma"] * self.volume_ratio:
                return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        
        if latest["close"] < support:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
