# -*- coding: utf-8 -*-
"""
分析器基类
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np


@dataclass
class AnalysisResult:
    """分析结果基类"""
    timestamp: datetime = field(default_factory=datetime.now)
    success: bool = True
    error_msg: str = ""
    
    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "success": self.success,
            "error_msg": self.error_msg,
        }


@dataclass
class ScoreResult(AnalysisResult):
    """评分结果"""
    score: float = 0.0
    raw_data: Dict[str, Any] = field(default_factory=dict)
    signals: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        data = super().to_dict()
        data.update({
            "score": self.score,
            "signals": self.signals,
            "warnings": self.warnings,
        })
        return data


class BaseAnalyzer(ABC):
    """分析器基类"""
    
    def __init__(self, name: str = "base_analyzer"):
        self.name = name
        self._cache: Dict[str, Any] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._cache_ttl = 300
    
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache_time:
            return False
        elapsed = (datetime.now() - self._cache_time[key]).total_seconds()
        return elapsed < self._cache_ttl
    
    def _get_cache(self, key: str) -> Optional[Any]:
        if self._is_cache_valid(key):
            return self._cache.get(key)
        return None
    
    def _set_cache(self, key: str, value: Any):
        self._cache[key] = value
        self._cache_time[key] = datetime.now()
    
    def clear_cache(self):
        self._cache.clear()
        self._cache_time.clear()
    
    @abstractmethod
    def analyze(self, **kwargs) -> AnalysisResult:
        """执行分析"""
        pass
    
    def _normalize_score(self, score: float, min_val: float = 0, max_val: float = 100) -> float:
        """归一化评分到0-100"""
        return max(0, min(100, (score - min_val) / (max_val - min_val) * 100)) if max_val > min_val else 50
    
    def _detect_trend(self, series: pd.Series, window: int = 5) -> str:
        """检测趋势"""
        if len(series) < window:
            return "震荡"
        recent = series.tail(window)
        ma = recent.mean()
        current = series.iloc[-1]
        if current > ma * 1.02:
            return "上涨"
        elif current < ma * 0.98:
            return "下跌"
        return "震荡"
    
    def _calc_momentum(self, series: pd.Series, periods: List[int] = [5, 10, 20]) -> Dict[str, float]:
        """计算动量"""
        result = {}
        for p in periods:
            if len(series) >= p:
                result[f"mom_{p}d"] = (series.iloc[-1] / series.iloc[-p] - 1) * 100
        return result
