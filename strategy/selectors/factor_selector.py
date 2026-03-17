# -*- coding: utf-8 -*-
"""
多因子选股策略
基于多个因子进行综合选股
"""
from typing import Dict, List, Optional, Callable
import pandas as pd

from strategy.selectors.base_selector import BaseSelector, MultiFactorSelector, SelectResult, StockScore


class FactorSelector(MultiFactorSelector):
    """
    多因子选股策略
    
    支持的因子:
    - 动量因子 (momentum)
    - 波动率因子 (volatility)
    - 成交量因子 (volume)
    - 趋势因子 (trend)
    """
    
    def __init__(
        self,
        factors: Optional[Dict[str, float]] = None,
        period: int = 20,
    ):
        super().__init__("Factor")
        self.period = period
        
        if factors is None:
            factors = {
                "momentum": 1.0,
                "volatility": -0.5,
                "volume": 0.5,
                "trend": 1.0,
            }
        self.factor_weights = factors
        
        self._init_factors()
    
    def _init_factors(self):
        """初始化因子计算函数"""
        self.add_factor("momentum", self._calc_momentum, self.factor_weights.get("momentum", 1.0))
        self.add_factor("volatility", self._calc_volatility, self.factor_weights.get("volatility", -0.5))
        self.add_factor("volume", self._calc_volume_factor, self.factor_weights.get("volume", 0.5))
        self.add_factor("trend", self._calc_trend, self.factor_weights.get("trend", 1.0))
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """多因子选股"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.period:
                continue
            
            factor_values = self.calc_factor_values(df)
            if factor_values.empty or factor_values.isna().all():
                continue
            
            normalized = self.normalize(factor_values)
            
            total_score = 0.0
            for name, weight in self.weights.items():
                if name in normalized.index and not pd.isna(normalized[name]):
                    total_score += normalized[name] * weight
            
            reason = f"动量:{factor_values.get('momentum', 0):.2f} 波动:{factor_values.get('volatility', 0):.2f}"
            scores.append(StockScore(
                symbol=symbol,
                score=total_score,
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
        """动量因子"""
        if "close" not in df.columns:
            return 0.0
        return (df["close"].iloc[-1] / df["close"].iloc[-self.period] - 1)
    
    def _calc_volatility(self, df: pd.DataFrame) -> float:
        """波动率因子 (波动率越低越好)"""
        if "close" not in df.columns or len(df) < self.period:
            return 0.0
        returns = df["close"].pct_change().iloc[-self.period:]
        return returns.std()
    
    def _calc_volume_factor(self, df: pd.DataFrame) -> float:
        """成交量因子 (成交量放大越好)"""
        if "volume" not in df.columns or len(df) < self.period:
            return 0.0
        vol_ma = df["volume"].rolling(self.period).mean().iloc[-1]
        current_vol = df["volume"].iloc[-1]
        return current_vol / vol_ma if vol_ma > 0 else 0.0
    
    def _calc_trend(self, df: pd.DataFrame) -> float:
        """趋势因子 (价格位于均线上方越好)"""
        if "close" not in df.columns:
            return 0.0
        ma = df["close"].rolling(self.period).mean().iloc[-1]
        return (df["close"].iloc[-1] / ma - 1) if ma > 0 else 0.0


class QualitySelector(MultiFactorSelector):
    """
    质量因子选股策略
    
    基于质量因子的选股:
    - ROE (盈利能力)
    - 毛利率
    - 资产负债率
    """
    
    def __init__(
        self,
        period: int = 20,
    ):
        super().__init__("Quality")
        self.period = period
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """质量因子选股 (简化版，仅使用技术指标)"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.period:
                continue
            
            quality_score = self._calc_quality_score(df)
            if pd.isna(quality_score):
                continue
            
            scores.append(StockScore(
                symbol=symbol,
                score=quality_score,
                reason=f"质量评分: {quality_score:.2f}"
            ))
        
        scores.sort(key=lambda x: x.score, reverse=True)
        
        for i, s in enumerate(scores):
            s.rank = i + 1
        
        return SelectResult(
            stocks=scores[:top_n],
            pool_size=len(symbols),
            selected_count=len(scores[:top_n])
        )
    
    def _calc_quality_score(self, df: pd.DataFrame) -> float:
        """计算质量评分 (简化版)"""
        if "close" not in df.columns or "volume" not in df.columns:
            return 0.0
        
        returns = df["close"].pct_change().iloc[-self.period:]
        
        profitability = returns[returns > 0].sum() / self.period
        
        volatility = returns.std()
        stability = 1 / (volatility + 0.001)
        
        volume_stability = df["volume"].rolling(10).std().iloc[-1] / (df["volume"].rolling(10).mean().iloc[-1] + 1)
        
        score = profitability * 0.4 + stability * 0.3 + (1 - volume_stability) * 0.3
        return score


class CompositeSelector(MultiFactorSelector):
    """
    综合选股策略
    
    结合动量、质量、趋势等多个维度
    """
    
    def __init__(
        self,
        momentum_weight: float = 0.4,
        quality_weight: float = 0.3,
        trend_weight: float = 0.3,
        period: int = 20,
    ):
        super().__init__("Composite")
        self.momentum_weight = momentum_weight
        self.quality_weight = quality_weight
        self.trend_weight = trend_weight
        self.period = period
    
    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """综合选股"""
        scores = []
        
        for symbol in symbols:
            df = self.get_data(symbol)
            if df is None or df.empty or len(df) < self.period:
                continue
            
            momentum = self._calc_momentum(df)
            quality = self._calc_quality(df)
            trend = self._calc_trend(df)
            
            if pd.isna(momentum) or pd.isna(quality) or pd.isna(trend):
                continue
            
            total_score = (
                momentum * self.momentum_weight +
                quality * self.quality_weight +
                trend * self.trend_weight
            )
            
            scores.append(StockScore(
                symbol=symbol,
                score=total_score,
                reason=f"动量:{momentum:.2f} 质量:{quality:.2f} 趋势:{trend:.2f}"
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
        """动量因子"""
        if "close" not in df.columns:
            return 0.0
        return (df["close"].iloc[-1] / df["close"].iloc[-self.period] - 1)
    
    def _calc_quality(self, df: pd.DataFrame) -> float:
        """质量因子"""
        if "close" not in df.columns:
            return 0.0
        returns = df["close"].pct_change().iloc[-self.period:]
        return returns[returns > 0].sum() / self.period
    
    def _calc_trend(self, df: pd.DataFrame) -> float:
        """趋势因子"""
        if "close" not in df.columns:
            return 0.0
        ma = df["close"].rolling(self.period).mean().iloc[-1]
        return (df["close"].iloc[-1] / ma - 1) if ma > 0 else 0.0
