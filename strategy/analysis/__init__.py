# -*- coding: utf-8 -*-
"""
分析模块

多维度量化分析系统:
- 情绪分析: 大盘情绪、个股情绪、板块情绪
- 资金分析: 主力资金、北向资金
- 技术分析: 趋势、动量、量价
- 基本面分析: 估值、成长、盈利
"""
from .base_analyzer import BaseAnalyzer, AnalysisResult
from .multi_analyzer import MultiDimensionalAnalyzer
from .emotion.market_emotion import MarketEmotionAnalyzer
from .emotion.stock_emotion import StockEmotionAnalyzer
from .emotion.sector_emotion import SectorEmotionAnalyzer
from .space.space_score import SpaceScoreAnalyzer, SpaceScore, SpaceLevel
from .fund.fund_consistency import FundConsistencyAnalyzer, FundConsistencyResult

__all__ = [
    "BaseAnalyzer",
    "AnalysisResult",
    "MultiDimensionalAnalyzer",
    "MarketEmotionAnalyzer",
    "StockEmotionAnalyzer",
    "SectorEmotionAnalyzer",
    "SpaceScoreAnalyzer",
    "SpaceScore",
    "SpaceLevel",
    "FundConsistencyAnalyzer",
    "FundConsistencyResult",
]
