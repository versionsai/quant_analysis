# -*- coding: utf-8 -*-
"""
过热度模型

核心组成:
- 连板高度
- 涨停拥挤度
- 成交量异常
- 一致性
- 板块集中度

示例数据结构:
{
    "trade_date": "20260327",
    "overheat": 0.63,
    "risk_level": "warning",
    "components": {
        "leader_height": 0.7,
        "zt_crowding": 0.6,
        "volume_anomaly": 0.5,
        "consensus": 0.7,
        "sector_concentration": 0.65
    }
}
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from strategy.analysis.emotion.market_cycle import MarketCycleAnalyzer, MarketCycleSnapshot
from strategy.analysis.emotion.sector_strength import SectorStrengthAnalyzer, SectorStrengthSnapshot
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OverheatSnapshot:
    """过热度快照。"""

    trade_date: str
    overheat: float
    risk_level: str
    components: Dict[str, float] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "overheat": round(float(self.overheat or 0.0), 4),
            "risk_level": self.risk_level,
            "components": {key: round(float(val or 0.0), 4) for key, val in self.components.items()},
            "reasons": list(self.reasons),
        }


class OverheatAnalyzer:
    """统一过热度分析器。"""

    def __init__(self):
        self.market_cycle_analyzer = MarketCycleAnalyzer()
        self.sector_strength_analyzer = SectorStrengthAnalyzer()

    def analyze(self, trade_date: Optional[str] = None) -> OverheatSnapshot:
        """分析指定交易日的市场过热度。"""
        date_text = str(trade_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
        market_cycle: MarketCycleSnapshot = self.market_cycle_analyzer.analyze(trade_date=date_text)
        sector_strength: SectorStrengthSnapshot = self.sector_strength_analyzer.analyze(trade_date=date_text)

        leader_height = min(1.0, float(market_cycle.lb_max or 0) / 7.0)
        zt_crowding = min(1.0, float(market_cycle.zt_count or 0) / 80.0)
        volume_anomaly = max(0.0, min(1.0, (float(market_cycle.normalized_score or 50.0) - 45.0) / 45.0))
        consensus = max(0.0, min(1.0, (float(market_cycle.zt_count or 0) - float(market_cycle.dt_count or 0)) / 60.0))
        sector_concentration = self._calc_sector_concentration(sector_strength)

        components = {
            "leader_height": leader_height,
            "zt_crowding": zt_crowding,
            "volume_anomaly": volume_anomaly,
            "consensus": consensus,
            "sector_concentration": sector_concentration,
        }
        overheat = (
            0.25 * leader_height
            + 0.20 * zt_crowding
            + 0.20 * volume_anomaly
            + 0.20 * consensus
            + 0.15 * sector_concentration
        )
        risk_level = self._risk_level(overheat)
        reasons = self._build_reasons(market_cycle, components)
        return OverheatSnapshot(
            trade_date=date_text,
            overheat=overheat,
            risk_level=risk_level,
            components=components,
            reasons=reasons,
        )

    @staticmethod
    def _calc_sector_concentration(snapshot: SectorStrengthSnapshot) -> float:
        """计算板块集中度。"""
        rows = list(snapshot.top_sectors or [])
        if not rows:
            return 0.5
        top_two = rows[:2]
        top_two_score = sum(float(item.get("score", 0.0) or 0.0) for item in top_two) / (len(top_two) * 100.0)
        return max(0.0, min(1.0, top_two_score))

    @staticmethod
    def _risk_level(overheat: float) -> str:
        """将过热度映射为风险等级。"""
        if overheat >= 0.75:
            return "critical"
        if overheat >= 0.60:
            return "warning"
        if overheat >= 0.40:
            return "warm"
        return "normal"

    @staticmethod
    def _build_reasons(snapshot: MarketCycleSnapshot, components: Dict[str, float]) -> List[str]:
        """构建过热原因。"""
        reasons: List[str] = []
        if components["leader_height"] >= 0.7:
            reasons.append(f"连板高度偏高({snapshot.lb_max}板)")
        if components["zt_crowding"] >= 0.6:
            reasons.append(f"涨停拥挤度偏高({snapshot.zt_count}家)")
        if components["consensus"] >= 0.6:
            reasons.append(f"一致性过强(涨跌停差={snapshot.zt_count - snapshot.dt_count})")
        if not reasons:
            reasons.append("情绪仍可控，暂无显著过热特征")
        return reasons


def build_overheat_snapshot(trade_date: Optional[str] = None) -> Dict[str, object]:
    """构建过热度快照字典。"""
    return OverheatAnalyzer().analyze(trade_date=trade_date).to_dict()
