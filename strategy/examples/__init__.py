# -*- coding: utf-8 -*-
"""
示例策略
"""
from .ma_strategy import MAStrategy
from .dual_ma_strategy import DualMAStrategy
from .price_action import PriceActionStrategy, BreakoutStrategy
from .macd_strategy import MACDStrategy, MACDTrendStrategy, MACDRSIStrategy
from .combined_strategy import PriceActionMACDStrategy, MultiTimeframeStrategy
from .taco_strategy import (
    TACOStrategy,
    TACOOilStrategy,
    TacoStrategyParams,
    build_taco_hot_topics,
    build_taco_params,
    build_taco_snapshot,
)

__all__ = [
    "MAStrategy",
    "DualMAStrategy",
    "PriceActionStrategy",
    "BreakoutStrategy",
    "MACDStrategy",
    "MACDTrendStrategy",
    "MACDRSIStrategy",
    "PriceActionMACDStrategy",
    "MultiTimeframeStrategy",
    "TACOStrategy",
    "TACOOilStrategy",
    "TacoStrategyParams",
    "build_taco_hot_topics",
    "build_taco_params",
    "build_taco_snapshot",
]
