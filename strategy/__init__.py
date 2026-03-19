# -*- coding: utf-8 -*-
"""
策略模块

包含两大类策略:
- 选股策略 (selectors): 从股票池中选择优质股票
- 择时策略 (timing): 决定买入卖出时机
"""
from .base import BaseStrategy, Signal, Position, Portfolio, MultiFactorStrategy
from .examples import (
    MAStrategy, 
    DualMAStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    MACDStrategy,
    MACDTrendStrategy,
    MACDRSIStrategy,
    PriceActionMACDStrategy,
    MultiTimeframeStrategy,
)
from .selectors import (
    BaseSelector,
    MultiFactorSelector,
    StockScore,
    SelectResult,
    MomentumSelector,
    DualMomentumSelector,
    RotationSelector,
    FactorSelector,
    QualitySelector,
    CompositeSelector,
    WeakToStrongSelector,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
    WeakToStrongStage,
)

__all__ = [
    # 基类
    "BaseStrategy", "Signal", "Position", "Portfolio", 
    "MultiFactorStrategy",
    "BaseSelector", "MultiFactorSelector", "StockScore", "SelectResult",
    # 择时策略
    "MAStrategy", "DualMomentumSelector",
    "PriceActionStrategy", "BreakoutStrategy",
    "MACDStrategy", "MACDTrendStrategy", "MACDRSIStrategy",
    "PriceActionMACDStrategy", "MultiTimeframeStrategy",
    # 选股策略
    "MomentumSelector", "DualMomentumSelector", "RotationSelector",
    "FactorSelector", "QualitySelector", "CompositeSelector",
    # 弱转强策略
    "WeakToStrongSelector",
    "WeakToStrongTimingStrategy",
    "WeakToStrongParams",
    "WeakToStrongStage",
]
