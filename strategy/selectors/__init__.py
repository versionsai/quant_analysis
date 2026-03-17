# -*- coding: utf-8 -*-
"""
选股策略模块
"""
from .base_selector import BaseSelector, MultiFactorSelector, StockScore, SelectResult
from .momentum_selector import MomentumSelector, DualMomentumSelector, RotationSelector
from .factor_selector import FactorSelector, QualitySelector, CompositeSelector

__all__ = [
    "BaseSelector",
    "MultiFactorSelector", 
    "StockScore",
    "SelectResult",
    "MomentumSelector",
    "DualMomentumSelector", 
    "RotationSelector",
    "FactorSelector",
    "QualitySelector",
    "CompositeSelector",
]
