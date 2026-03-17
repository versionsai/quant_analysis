# -*- coding: utf-8 -*-
"""
策略模块
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

__all__ = [
    "BaseStrategy", "Signal", "Position", "Portfolio", 
    "MultiFactorStrategy", 
    "MAStrategy", "DualMAStrategy",
    "PriceActionStrategy", "BreakoutStrategy",
    "MACDStrategy", "MACDTrendStrategy", "MACDRSIStrategy",
    "PriceActionMACDStrategy", "MultiTimeframeStrategy",
]
