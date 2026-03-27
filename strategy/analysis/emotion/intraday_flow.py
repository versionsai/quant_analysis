# -*- coding: utf-8 -*-
"""
日内情绪流模块

基于盘中结构识别结果，抽象出统一的日内情绪方向与风险偏置。

示例数据结构:
{
    "trade_date": "20260327",
    "intraday_score": 0.58,
    "bias": "neutral",
    "trap_type": "fake_up",
    "fake_up_score": 0.71,
    "fake_down_score": 0.21
}
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from strategy.analysis.intraday.index_trap import IntradayTrapAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class IntradayFlowSnapshot:
    """日内情绪流快照。"""

    trade_date: str
    intraday_score: float
    bias: str
    trap_type: str
    fake_up_score: float = 0.0
    fake_down_score: float = 0.0
    breadth_comment: str = ""
    regime_comment: str = ""

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "intraday_score": round(float(self.intraday_score or 0.0), 4),
            "bias": self.bias,
            "trap_type": self.trap_type,
            "fake_up_score": round(float(self.fake_up_score or 0.0), 4),
            "fake_down_score": round(float(self.fake_down_score or 0.0), 4),
            "breadth_comment": self.breadth_comment,
            "regime_comment": self.regime_comment,
        }


class IntradayFlowAnalyzer:
    """统一日内情绪流分析器。"""

    def __init__(self):
        self._analyzer = IntradayTrapAnalyzer()

    def analyze(self, as_of: Optional[datetime] = None) -> IntradayFlowSnapshot:
        """分析指定时点的日内情绪流。"""
        ts = as_of or datetime.now()
        signal = self._analyzer.analyze_market_intraday(as_of=ts)
        bias, score = self._normalize_bias(signal.trap_type, float(signal.fake_up_score or 0.0), float(signal.fake_down_score or 0.0))
        return IntradayFlowSnapshot(
            trade_date=ts.strftime("%Y%m%d"),
            intraday_score=score,
            bias=bias,
            trap_type=str(signal.trap_type or "neutral"),
            fake_up_score=float(signal.fake_up_score or 0.0),
            fake_down_score=float(signal.fake_down_score or 0.0),
            breadth_comment=str(signal.breadth_comment or ""),
            regime_comment=str(signal.regime_comment or ""),
        )

    @staticmethod
    def _normalize_bias(trap_type: str, fake_up_score: float, fake_down_score: float) -> tuple[str, float]:
        """将盘中结构结果归一化为 0~1 的情绪分。"""
        raw = str(trap_type or "neutral").strip()
        if raw == "true_break":
            return "risk_on", 0.75
        if raw == "true_drop":
            return "risk_off", 0.20
        if raw == "fake_up":
            return "risk_off", max(0.10, 0.55 - fake_up_score * 0.35)
        if raw == "fake_down":
            return "risk_on", min(0.85, 0.45 + fake_down_score * 0.25)
        if raw == "chaotic":
            return "chaotic", 0.40
        return "neutral", 0.50


def build_intraday_flow_snapshot(as_of: Optional[datetime] = None) -> Dict[str, object]:
    """构建日内情绪流快照字典。"""
    return IntradayFlowAnalyzer().analyze(as_of=as_of).to_dict()

