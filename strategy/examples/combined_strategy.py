# -*- coding: utf-8 -*-
"""
Price Action + MACD 复合策略
结合价格行为和MACD指标的策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime
from typing import Dict


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
        market_context = self.get_market_context()
        market_profile = self._get_market_profile(market_context)
        
        buy_score = self._calc_buy_score(df)
        sell_score = self._calc_sell_score(df)
        buy_candidate_score = self._calc_market_adjusted_candidate_score(buy_score, df, market_context, side="buy")
        sell_candidate_score = self._calc_market_adjusted_candidate_score(sell_score, df, market_context, side="sell")
        allow_buy = self._allow_regime_buy(df, buy_score, buy_candidate_score, market_context)
        buy_reason = (
            f"{market_profile['label']} buy_score={buy_score} "
            f"candidate_score={buy_candidate_score:.2f} threshold={market_profile['buy_candidate_threshold']:.2f}"
        )
        
        if self.require_confirmation:
            if (
                buy_score >= market_profile["buy_score_threshold"]
                and buy_candidate_score >= market_profile["buy_candidate_threshold"]
                and allow_buy
            ):
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=1,
                    weight=min((buy_score / 3) * market_profile["weight_scale"], 1.0),
                    candidate_score=buy_candidate_score,
                    gate_passed=True,
                    gate_reason=buy_reason,
                )
            elif sell_score >= 2:
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=-1,
                    weight=min(sell_score / 3, 1.0),
                    candidate_score=sell_candidate_score,
                    gate_passed=True,
                    gate_reason=f"{market_profile['label']} sell_score={sell_score}",
                )
        else:
            if (
                buy_score >= max(1, market_profile["buy_score_threshold"] - 1)
                and latest["macd"] > 0
                and buy_candidate_score >= max(0.40, market_profile["buy_candidate_threshold"] - 0.08)
                and allow_buy
            ):
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=1,
                    weight=min(0.8 * market_profile["weight_scale"], 1.0),
                    candidate_score=buy_candidate_score,
                    gate_passed=True,
                    gate_reason=buy_reason,
                )
            elif sell_score >= 1 and latest["macd"] < 0:
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=-1,
                    weight=0.8,
                    candidate_score=sell_candidate_score,
                    gate_passed=True,
                    gate_reason=f"{market_profile['label']} sell_score={sell_score}",
                )
        
        return Signal(
            symbol=symbol,
            date=datetime.now(),
            signal=0,
            weight=0.0,
            candidate_score=max(buy_candidate_score, sell_candidate_score) if max(buy_score, sell_score) > 0 else 0.0,
            gate_passed=False,
            gate_reason=f"{market_profile['label']} buy_score={buy_score}, sell_score={sell_score}",
        )
    
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

    def _get_market_profile(self, market_context: Dict[str, object]) -> Dict[str, float]:
        """获取市场状态画像。"""
        regime = str(market_context.get("regime", "normal") or "normal")
        if regime == "defense":
            return {
                "buy_score_threshold": 4,
                "buy_candidate_threshold": 0.72,
                "weight_scale": 0.50,
                "label": "defense",
            }
        if regime == "golden_pit":
            return {
                "buy_score_threshold": 3,
                "buy_candidate_threshold": 0.56,
                "weight_scale": 0.78,
                "label": "golden_pit",
            }
        return {
            "buy_score_threshold": 2,
            "buy_candidate_threshold": 0.45,
            "weight_scale": 1.0,
            "label": "normal",
        }

    def _calc_market_adjusted_candidate_score(
        self,
        signal_score: int,
        df: pd.DataFrame,
        market_context: Dict[str, object],
        side: str,
    ) -> float:
        """结合市场状态调整候选分。"""
        base_score = min(max(signal_score, 0) / 5, 1.0)
        latest = df.iloc[-1]
        market_score = float(market_context.get("market_score", 50.0) or 50.0)
        space_score = float(market_context.get("space_score", 50.0) or 50.0)
        regime = str(market_context.get("regime", "normal") or "normal")
        adjusted = base_score

        if side == "buy":
            adjusted += float(np.clip((space_score - 50.0) / 100.0, -0.10, 0.10))
            adjusted += float(np.clip((market_score - 45.0) / 140.0, -0.06, 0.08))
            if regime == "defense":
                adjusted -= 0.12
                if self._check_bottom_divergence(df):
                    adjusted += 0.08
            elif regime == "golden_pit":
                adjusted += 0.08
                if self._check_bottom_divergence(df):
                    adjusted += 0.06
        else:
            adjusted += 0.05 if regime == "defense" else 0.0

        hist_abs = float(abs(latest.get("macd_hist", 0.0)) or 0.0)
        adjusted += min(hist_abs * 8.0, 0.08)
        return float(np.clip(adjusted, 0.0, 1.0))

    def _allow_regime_buy(
        self,
        df: pd.DataFrame,
        buy_score: int,
        candidate_score: float,
        market_context: Dict[str, object],
    ) -> bool:
        """按市场状态限制买入结构。"""
        regime = str(market_context.get("regime", "normal") or "normal")
        latest = df.iloc[-1]
        bullish_pinbar = self._is_bullish_pinbar(latest)
        bottom_divergence = self._check_bottom_divergence(df)
        breakout = bool(latest.get("close", 0.0) > latest.get("swing_high", latest.get("close", 0.0)))
        histogram_turn_up = float(latest.get("macd_hist", 0.0) or 0.0) > float(df.iloc[-2].get("macd_hist", 0.0) or 0.0)

        if regime == "defense":
            return bool(
                candidate_score >= 0.78
                and (
                    bottom_divergence
                    or (bullish_pinbar and breakout)
                    or (buy_score >= 5 and breakout and histogram_turn_up)
                )
            )
        if regime == "golden_pit":
            return bool(bottom_divergence or bullish_pinbar or (breakout and histogram_turn_up))
        return True


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
