# -*- coding: utf-8 -*-
"""
推送报告格式化模块
统一管理综合报告的结构与文案格式
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


STATUS_LABELS: Dict[str, str] = {
    "pending": "待执行",
    "holding": "持有中",
    "sold": "已卖出",
    "closed": "已结束",
    "skip": "已跳过",
    "buy": "买入中",
    "sell": "卖出中",
}


def to_status_label(status: str) -> str:
    """状态中文化"""
    raw = str(status or "").strip()
    if not raw:
        return ""
    return STATUS_LABELS.get(raw, raw)


@dataclass
class HoldingReportRow:
    """持仓分析行"""

    code: str
    name: str
    latest_price: float = 0.0
    pnl_pct: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    factor_text: str = "量化因子: 暂无"
    fundamental_text: str = "基本面: 暂无"
    tech_text: str = "技术面: 暂无"
    fund_text: str = "资金面: 暂无"
    emotion_text: str = "情绪面: 暂无"


@dataclass
class DecisionReportRow:
    """决策分析行"""

    code: str
    name: str
    action: str = "保持不变"
    reasons: List[str] = field(default_factory=list)


@dataclass
class SignalRecommendRow:
    """信号推荐行"""

    code: str
    name: str
    price: float = 0.0
    change_pct: float = 0.0
    signal: str = "观望"
    target: Optional[float] = None
    stop_loss: Optional[float] = None
    reason: str = ""
    dual_signal: bool = False
    ws_stage: int = 0


@dataclass
class NewsReportBlock:
    """新闻分析子区块"""

    title: str
    content: str


@dataclass
class ReviewTradeRow:
    """复盘成交行"""

    date: str
    code: str
    direction: str
    price: float = 0.0
    pnl: float = 0.0


@dataclass
class TradeTimelineRow:
    """交易时间线行"""

    date: str
    code: str
    name: str
    event_type: str = ""
    signal_type: str = ""
    price: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    quantity: int = 0
    reason: str = ""
    status: str = ""
    pnl: float = 0.0
    pnl_pct: float = 0.0


@dataclass
class TradeLifecycleRow:
    """交易生命周期摘要行"""

    code: str
    name: str
    open_cost: float = 0.0
    holding_quantity: int = 0
    latest_price: float = 0.0
    floating_pnl: float = 0.0
    floating_pnl_pct: float = 0.0
    realized_pnl: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0


@dataclass
class ProxyDiffRow:
    """主线强度偏差行"""

    code: str
    name: str
    real_concept_name: str = "-"
    real_score: float = 0.0
    proxy_score: float = 0.0
    diff_score: float = 0.0


def format_news_section(
    blocks: Optional[List[NewsReportBlock]] = None,
) -> str:
    """格式化新闻分析区块"""
    lines: List[str] = ["【新闻分析】"]
    for block in blocks or []:
        if not block.content:
            continue
        lines.append(block.title)
        lines.append(block.content)
    if len(lines) == 1:
        lines.append("暂无新闻数据")
    return "\n".join(lines)


def format_holdings_section(rows: List[HoldingReportRow]) -> str:
    """格式化持仓分析区块"""
    if not rows:
        return "【持仓分析】\n当前空仓"

    lines: List[str] = ["【持仓分析】"]
    for row in rows:
        lines.append(
            f"{row.code} {row.name} | "
            f"最新价{float(row.latest_price):.2f} | "
            f"盈亏{float(row.pnl_pct):+.2f}% | "
            f"预期止盈/止损 {float(row.target_price):.2f}/{float(row.stop_loss):.2f}"
        )
        lines.append(str(row.factor_text))
        lines.append(str(row.fundamental_text))
        lines.append(str(row.tech_text))
        lines.append(str(row.fund_text))
        lines.append(str(row.emotion_text))
        lines.append("-" * 24)
    return "\n".join(lines)


def format_decision_section(rows: List[DecisionReportRow]) -> str:
    """格式化决策分析区块"""
    if not rows:
        return "【决策分析】\n当前空仓，无需加仓或清仓"

    lines: List[str] = ["【决策分析】"]
    for row in rows:
        reasons = row.reasons or ["暂无额外说明"]
        lines.append(
            f"{row.code} {row.name} -> {row.action} | "
            f"原因: {'; '.join([str(x) for x in reasons[:3]])}"
        )
    return "\n".join(lines)


def format_signal_section(etf_recs: List[SignalRecommendRow], stock_recs: List[SignalRecommendRow]) -> str:
    """格式化信号推荐区块"""
    rows = [
        "【信号推荐】",
        "| 代码 | 名称 | 信号来源 | 操作 | 预测止盈/止损点 | 是否重点关注 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    all_recs = (stock_recs or [])[:5] + (etf_recs or [])[:3]
    for rec in all_recs:
        target = rec.target
        stop_loss = rec.stop_loss
        risk_text = "-"
        if target and stop_loss:
            risk_text = f"{float(target):.2f}/{float(stop_loss):.2f}"
        reason = str(rec.reason or "-").replace("\n", " ")
        focus = "是" if rec.dual_signal else "否"
        rows.append(
            f"| {rec.code} | {rec.name} | {reason[:24]} | "
            f"{rec.signal} | {risk_text} | {focus} |"
        )

    if len(rows) == 3:
        rows.append("| - | - | - | 观望 | - | 否 |")
    return "\n".join(rows)


def format_review_section(
    stats: Dict,
    trades: List[ReviewTradeRow],
    proxy_diff_rows: Optional[List[ProxyDiffRow]] = None,
) -> str:
    """格式化回测复盘区块"""
    lines = ["【回测复盘】"]
    lines.append(
        f"卖出统计: 交易{int(stats.get('total_trades', 0))}次 | "
        f"胜率{float(stats.get('win_rate', 0.0)):.1f}% | "
        f"总收益{float(stats.get('total_pnl', 0.0)):.2f} | "
        f"平均收益{float(stats.get('avg_pnl', 0.0)):.2f}"
    )
    if trades:
        lines.append("最近成交")
        for trade in trades[:5]:
            lines.append(
                f"{trade.date} {trade.code} "
                f"{trade.direction} @ {float(trade.price):.2f} "
                f"PnL {float(trade.pnl):.2f}"
            )
    else:
        lines.append("最近无成交记录")

    if proxy_diff_rows:
        lines.append("主线强度偏差")
        for row in proxy_diff_rows[:5]:
            lines.append(
                f"{row.code} {row.name} | 实盘概念{row.real_concept_name}:{row.real_score:.2f} | "
                f"回测代理:{row.proxy_score:.2f} | 偏差{row.diff_score:+.2f}"
            )
    return "\n".join(lines)


def format_trade_timeline_section(rows: List[TradeTimelineRow]) -> str:
    """格式化交易时间线区块"""
    if not rows:
        return "【交易时间线】\n暂无买卖点记录"

    event_type_labels = {
        "recommend": "荐股",
        "buy": "买入",
        "sell": "卖出",
        "scale_out": "减仓",
        "skip": "跳过",
    }

    grouped: Dict[str, List[TradeTimelineRow]] = {}
    for row in rows:
        grouped.setdefault(row.code, []).append(row)

    lines: List[str] = ["【交易时间线】"]
    for code, events in grouped.items():
        header = events[0]
        lines.append(f"{code} {header.name}")
        for event in events:
            event_label = event_type_labels.get(event.event_type, event.event_type or "-")
            parts: List[str] = [f"  {event.date}", event_label]
            if event.signal_type:
                parts.append(event.signal_type)
            if float(event.price or 0) > 0:
                parts.append(f"价格{float(event.price):.2f}")
            if float(event.target_price or 0) > 0 or float(event.stop_loss or 0) > 0:
                parts.append(
                    f"止盈/止损{float(event.target_price or 0):.2f}/{float(event.stop_loss or 0):.2f}"
                )
            if int(event.quantity or 0) > 0:
                parts.append(f"数量{int(event.quantity)}")
            if abs(float(event.pnl or 0)) > 0:
                parts.append(f"收益{float(event.pnl):+.2f}")
            if abs(float(event.pnl_pct or 0)) > 0:
                parts.append(f"收益率{float(event.pnl_pct):+.2f}%")
            if event.status:
                parts.append(f"状态{to_status_label(event.status)}")
            if event.reason:
                parts.append(f"原因:{event.reason}")
            lines.append(" | ".join(parts))
        lines.append("-" * 24)

    if lines[-1] == "-" * 24:
        lines.pop()
    return "\n".join(lines)


def format_trade_lifecycle_section(rows: List[TradeLifecycleRow]) -> str:
    """格式化交易生命周期摘要区块"""
    if not rows:
        return "【交易摘要】\n暂无交易摘要"

    lines: List[str] = ["【交易摘要】"]
    for row in rows:
        lines.append(
            f"{row.code} {row.name} | 开仓成本{float(row.open_cost):.2f} | "
            f"持仓{int(row.holding_quantity)} | 最新价{float(row.latest_price):.2f}"
        )
        lines.append(
            f"  浮盈{float(row.floating_pnl):+.2f} ({float(row.floating_pnl_pct):+.2f}%) | "
            f"已实现{float(row.realized_pnl):+.2f} | "
            f"总收益{float(row.total_pnl):+.2f} ({float(row.total_pnl_pct):+.2f}%)"
        )
        lines.append("-" * 24)

    if lines[-1] == "-" * 24:
        lines.pop()
    return "\n".join(lines)
