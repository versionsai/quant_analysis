# -*- coding: utf-8 -*-
"""
Price Action 策略
基于价格行为的交易策略
"""
import pandas as pd
import numpy as np
from strategy.base import BaseStrategy, Signal
from datetime import datetime
from typing import Dict


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
        breakout_hit = self._check_breakout(df)
        breakdown_hit = self._check_breakdown(df)
        pinbar_hit = self._check_pinbar(df)
        reversal_hit = self._check_trend_reversal(df)
        market_context = self.get_market_context()
        market_profile = self._get_market_profile(market_context)
        candidate_score = self._calc_candidate_score(
            df,
            breakout_hit,
            breakdown_hit,
            pinbar_hit,
            reversal_hit,
            market_context,
        )
        
        if any(pd.isna([latest.get(k, 0) for k in ['swing_high', 'swing_low', 'atr', 'ema20']])):
            return None
        
        if reversal_hit or breakdown_hit:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=-1,
                weight=1.0,
                candidate_score=candidate_score,
                gate_passed=True,
                gate_reason=f"趋势反转或跌破关键位|{market_profile['label']}",
            )

        if breakout_hit or pinbar_hit:
            allow_buy = self._allow_regime_buy(df, breakout_hit, pinbar_hit, candidate_score, market_context)
            gate_reason = (
                f"{market_profile['label']} candidate_score={candidate_score:.2f} "
                f"threshold={market_profile['buy_threshold']:.2f}"
            )
            if allow_buy and candidate_score >= market_profile["buy_threshold"]:
                weight = min(1.0, market_profile["weight_scale"])
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=1,
                    weight=weight,
                    candidate_score=candidate_score,
                    gate_passed=True,
                    gate_reason=f"突破或Pin Bar结构成立|{gate_reason}",
                )
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=0,
                weight=0.0,
                candidate_score=candidate_score,
                gate_passed=False,
                gate_reason=f"突破或Pin Bar结构存在但未通过市场自适应门槛|{gate_reason}",
            )
        
        return Signal(
            symbol=symbol,
            date=datetime.now(),
            signal=0,
            weight=0.0,
            candidate_score=candidate_score,
            gate_passed=False,
            gate_reason=f"价格行为结构未确认|{market_profile['label']}",
        )

    def _calc_candidate_score(
        self,
        df: pd.DataFrame,
        breakout_hit: bool,
        breakdown_hit: bool,
        pinbar_hit: bool,
        reversal_hit: bool,
        market_context: Dict[str, object],
    ) -> float:
        """计算 Price Action 候选分。"""
        latest = df.iloc[-1]
        score = 0.0
        if breakout_hit or breakdown_hit:
            score += 0.55
        if pinbar_hit or reversal_hit:
            score += 0.25
        body_ratio = float(latest.get("body_ratio", 0.0) or 0.0)
        score += min(max(body_ratio - self.min_body_ratio, 0.0), 0.2)
        regime = str(market_context.get("regime", "normal") or "normal")
        market_score = float(market_context.get("market_score", 50.0) or 50.0)
        space_score = float(market_context.get("space_score", 50.0) or 50.0)
        if breakout_hit:
            score += float(np.clip((space_score - 50.0) / 100.0, -0.08, 0.10))
        if pinbar_hit:
            score += float(np.clip((market_score - 45.0) / 120.0, -0.05, 0.08))
        if regime == "defense":
            score -= 0.12
            if pinbar_hit:
                score += 0.05
        elif regime == "golden_pit":
            score += 0.06
            if breakout_hit:
                score += 0.04
        return float(min(score, 1.0))

    def _get_market_profile(self, market_context: Dict[str, object]) -> Dict[str, float]:
        """获取市场状态画像。"""
        regime = str(market_context.get("regime", "normal") or "normal")
        if regime == "defense":
            return {"buy_threshold": 0.72, "weight_scale": 0.55, "label": "defense"}
        if regime == "golden_pit":
            return {"buy_threshold": 0.58, "weight_scale": 0.80, "label": "golden_pit"}
        return {"buy_threshold": 0.52, "weight_scale": 1.00, "label": "normal"}

    def _allow_regime_buy(
        self,
        df: pd.DataFrame,
        breakout_hit: bool,
        pinbar_hit: bool,
        candidate_score: float,
        market_context: Dict[str, object],
    ) -> bool:
        """按市场状态限制买入结构。"""
        regime = str(market_context.get("regime", "normal") or "normal")
        latest = df.iloc[-1]
        volume_ma10 = float(df["volume"].rolling(10).mean().iloc[-1]) if "volume" in df.columns and len(df) >= 10 else 0.0
        volume = float(latest.get("volume", 0.0) or 0.0)
        vol_ratio = (volume / volume_ma10) if volume_ma10 > 0 else 1.0
        close_strength = (float(latest.get("close", 0.0) or 0.0) - float(latest.get("low", 0.0) or 0.0)) / max(
            float(latest.get("high", 0.0) or 0.0) - float(latest.get("low", 0.0) or 0.0),
            1e-6,
        )

        if regime == "defense":
            return bool(
                (breakout_hit and pinbar_hit)
                or (breakout_hit and vol_ratio >= 1.2 and close_strength >= 0.70 and candidate_score >= 0.80)
            )
        if regime == "golden_pit":
            return bool(pinbar_hit or (breakout_hit and close_strength >= 0.60))
        return bool(breakout_hit or pinbar_hit)
    
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
                return Signal(
                    symbol=symbol,
                    date=datetime.now(),
                    signal=1,
                    weight=1.0,
                    candidate_score=0.85,
                    gate_passed=True,
                    gate_reason="放量突破区间上沿",
                )
        
        if latest["close"] < support:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=-1,
                weight=1.0,
                candidate_score=0.8,
                gate_passed=True,
                gate_reason="跌破区间下沿",
            )
        
        candidate_score = 0.4 if price_range >= latest["close"] * 0.02 else 0.0
        return Signal(
            symbol=symbol,
            date=datetime.now(),
            signal=0,
            weight=0.0,
            candidate_score=candidate_score,
            gate_passed=False,
            gate_reason="区间突破未确认",
        )
