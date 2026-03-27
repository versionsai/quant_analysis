# -*- coding: utf-8 -*-
"""
短线情绪策略

基于以下规则生成交易决策：

BUY:
  space_score > 0.6 AND top_prob < 0.4

SELL:
  top_prob > 0.6

该文件只负责策略逻辑与特征适配，不承担回测撮合职责。
"""
from dataclasses import dataclass
from typing import Dict, Optional

from strategy.ml import TopFeatureRow, TopPredictModel


@dataclass
class EmotionStrategyDecision:
    """单日单票策略决策。"""

    symbol: str
    trade_date: str
    action: str
    top_prob: float
    space_score: float
    overheat: float
    reason: str

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "symbol": self.symbol,
            "trade_date": self.trade_date,
            "action": self.action,
            "top_prob": round(float(self.top_prob or 0.0), 4),
            "space_score": round(float(self.space_score or 0.0), 4),
            "overheat": round(float(self.overheat or 0.0), 4),
            "reason": self.reason,
        }


class EmotionTopStrategy:
    """基于情绪与 Top 风险概率的短线策略。"""

    def __init__(
        self,
        model: TopPredictModel,
        buy_threshold: float = 0.4,
        sell_threshold: float = 0.6,
        min_space_score: float = 0.6,
    ):
        self.model = model
        self.buy_threshold = float(buy_threshold)
        self.sell_threshold = float(sell_threshold)
        self.min_space_score = float(min_space_score)

    def evaluate(self, feature_row: TopFeatureRow | Dict[str, object]) -> EmotionStrategyDecision:
        """对单条特征进行策略评估。"""
        row = self._normalize_row(feature_row)
        prediction = self.model.predict_one(row)

        action = "hold"
        if row.space_score > self.min_space_score and prediction.top_prob < self.buy_threshold:
            action = "buy"
        elif prediction.top_prob > self.sell_threshold:
            action = "sell"

        reason = (
            f"space_score={row.space_score:.2f}, overheat={row.overheat:.2f}, "
            f"top_prob={prediction.top_prob:.2f}"
        )
        return EmotionStrategyDecision(
            symbol=row.symbol,
            trade_date=row.trade_date,
            action=action,
            top_prob=float(prediction.top_prob or 0.0),
            space_score=float(row.space_score or 0.0),
            overheat=float(row.overheat or 0.0),
            reason=reason,
        )

    @staticmethod
    def _normalize_row(feature_row: TopFeatureRow | Dict[str, object]) -> TopFeatureRow:
        """标准化输入特征。"""
        if isinstance(feature_row, TopFeatureRow):
            return feature_row
        item = dict(feature_row or {})
        return TopFeatureRow(
            trade_date=str(item.get("trade_date", "") or ""),
            symbol=str(item.get("symbol", "") or ""),
            space_score=float(item.get("space_score", 0.0) or 0.0),
            overheat=float(item.get("overheat", 0.0) or 0.0),
            acc=float(item.get("acc", 0.0) or 0.0),
            zt_diff=float(item.get("zt_diff", 0.0) or 0.0),
            eff_diff=float(item.get("eff_diff", 0.0) or 0.0),
            leader_ret=float(item.get("leader_ret", 0.0) or 0.0),
            label=int(item["label"]) if item.get("label") is not None else None,
        )
