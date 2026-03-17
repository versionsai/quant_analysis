# -*- coding: utf-8 -*-
"""
Price Action + MACD 复合策略
结合价格行为和MACD指标的策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime


class PriceActionMACDStrategy(BaseStrategy):
    """
    Price Action + MACD 复合策略
    
    买入信号 (满足条件越多，信号越强):
    1. MACD金叉 + 价格突破区间高点
    2. MACD零轴上方 + Pin Bar买入形态
    3. MACD底背离 + 价格触及支撑位
    
    卖出信号:
    1. MACD死叉 + 价格跌破区间低点
    2. MACD零轴下方 + Pin Bar卖出形态
    3. MACD顶背离 + 价格触及压力位
    """
    
    def __init__(
        self,
        lookback: int = 20,
        macd_fast: int = 12,
        macd_slow: int = 26,
        macd_signal: int = 9,
        require_confirmation: bool = True,
    ):
        super().__init__("PA_MACD")
        self.lookback = lookback
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.require_confirmation = require_confirmation
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < max(self.lookback, self.macd_slow) + 5:
            return None
        
        df = df.copy()
        df = self._calc_indicators(df)
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        buy_score = self._calc_buy_score(df)
        sell_score = self._calc_sell_score(df)
        
        if self.require_confirmation:
            if buy_score >= 2:
                return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=min(buy_score / 3, 1.0))
            elif sell_score >= 2:
                return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=min(sell_score / 3, 1.0))
        else:
            if buy_score >= 1 and latest["macd"] > 0:
                return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=0.8)
            elif sell_score >= 1 and latest["macd"] < 0:
                return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=0.8)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
    
    def _calc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有指标"""
        df["high"] = df.get("high", df["close"])
        df["low"] = df.get("low", df["close"])
        
        df["swing_high"] = df["high"].rolling(self.lookback).max()
        df["swing_low"] = df["low"].rolling(self.lookback).min()
        
        df["ema20"] = df["close"].ewm(span=20).mean()
        
        exp1 = df["close"].ewm(span=self.macd_fast, adjust=False).mean()
        exp2 = df["close"].ewm(span=self.macd_slow, adjust=False).mean()
        df["macd"] = exp1 - exp2
        df["macd_signal"] = df["macd"].ewm(span=self.macd_signal, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        
        high_low = df["high"] - df["low"]
        high_close = abs(df["high"] - df["close"].shift())
        low_close = abs(df["low"] - df["close"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = tr.rolling(14).mean()
        
        df["body"] = abs(df["close"] - df["open"])
        df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
        df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]
        
        return df
    
    def _calc_buy_score(self, df: pd.DataFrame) -> int:
        """计算买入信号强度"""
        score = 0
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if prev["macd"] <= prev["macd_signal"] and latest["macd"] > latest["macd_signal"]:
            score += 1
        
        if latest["macd"] > 0 and latest["macd_signal"] > 0:
            score += 1
        
        if latest["close"] > latest["swing_high"]:
            score += 1
        
        if latest["close"] > latest["ema20"] and latest["ema20"] > df["ema20"].shift(1).iloc[-1]:
            score += 1
        
        if self._is_bullish_pinbar(latest):
            score += 1
        
        if self._check_bottom_divergence(df):
            score += 2
        
        return score
    
    def _calc_sell_score(self, df: pd.DataFrame) -> int:
        """计算卖出信号强度"""
        score = 0
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        if prev["macd"] >= prev["macd_signal"] and latest["macd"] < latest["macd_signal"]:
            score += 1
        
        if latest["macd"] < 0:
            score += 1
        
        if latest["close"] < latest["swing_low"]:
            score += 1
        
        if latest["close"] < latest["ema20"] and latest["ema20"] < df["ema20"].shift(1).iloc[-1]:
            score += 1
        
        if self._is_bearish_pinbar(latest):
            score += 1
        
        if self._check_top_divergence(df):
            score += 2
        
        return score
    
    def _is_bullish_pinbar(self, row: pd.Series) -> bool:
        """是否为看涨Pin Bar"""
        body = row["body"]
        lower_shadow = row["lower_shadow"]
        total_range = row["high"] - row["low"] + 0.001
        
        if body < row["atr"] * 0.3:
            return False
        
        return lower_shadow > body * 1.5 and lower_shadow > total_range * 0.5
    
    def _is_bearish_pinbar(self, row: pd.Series) -> bool:
        """是否为看跌Pin Bar"""
        body = row["body"]
        upper_shadow = row["upper_shadow"]
        total_range = row["high"] - row["low"] + 0.001
        
        if body < row["atr"] * 0.3:
            return False
        
        return upper_shadow > body * 1.5 and upper_shadow > total_range * 0.5
    
    def _check_bottom_divergence(self, df: pd.DataFrame) -> bool:
        """检查底背离"""
        if len(df) < 20:
            return False
        
        prices = df["close"].values
        macd_vals = df["macd"].values
        
        price_low_1 = min(prices[-15:-10])
        price_low_2 = min(prices[-10:-5])
        macd_low_1 = min(macd_vals[-15:-10])
        macd_low_2 = min(macd_vals[-10:-5])
        
        return price_low_2 < price_low_1 and macd_low_2 > macd_low_1
    
    def _check_top_divergence(self, df: pd.DataFrame) -> bool:
        """检查顶背离"""
        if len(df) < 20:
            return False
        
        prices = df["close"].values
        macd_vals = df["macd"].values
        
        price_high_1 = max(prices[-15:-10])
        price_high_2 = max(prices[-10:-5])
        macd_high_1 = max(macd_vals[-15:-10])
        macd_high_2 = max(macd_vals[-10:-5])
        
        return price_high_2 > price_high_1 and macd_high_2 < macd_high_1


class MultiTimeframeStrategy(BaseStrategy):
    """
    多时间框架策略
    日线判断趋势，30分钟/60分钟找入场点
    """
    
    def __init__(
        self,
        trend_lookback: int = 50,
        entry_lookback: int = 20,
    ):
        super().__init__("MultiTF")
        self.trend_lookback = trend_lookback
        self.entry_lookback = entry_lookback
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Signal:
        if len(df) < self.trend_lookback:
            return None
        
        df = df.copy()
        
        df["ema50"] = df["close"].ewm(span=50).mean()
        df["ema20"] = df["close"].ewm(span=20).mean()
        
        df["swing_high"] = df["high"].rolling(self.entry_lookback).max()
        df["swing_low"] = df["low"].rolling(self.entry_lookback).min()
        
        exp1 = df["close"].ewm(span=12, adjust=False).mean()
        exp2 = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = exp1 - exp2
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        
        latest = df.iloc[-1]
        
        trend_up = latest["close"] > latest["ema50"] and latest["ema20"] > latest["ema50"]
        trend_down = latest["close"] < latest["ema50"] and latest["ema20"] < latest["ema50"]
        
        macd_golden = latest["macd"] > latest["macd_signal"]
        macd_death = latest["macd"] < latest["macd_signal"]
        
        if trend_up and macd_golden and latest["close"] > latest["swing_high"]:
            return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=1.0)
        
        if trend_down and macd_death and latest["close"] < latest["swing_low"]:
            return Signal(symbol=symbol, date=datetime.now(), signal=-1, weight=1.0)
        
        return Signal(symbol=symbol, date=datetime.now(), signal=0, weight=0.0)
