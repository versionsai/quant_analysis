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
    order_book_text: str = "暂无"
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


def format_news_section(blocks: Optional[List[NewsReportBlock]] = None) -> str:
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
    """格式化持仓分析区块（移动端友好版）"""
    if not rows:
        return "【持仓分析】\n当前空仓"

    lines: List[str] = ["【持仓分析】"]
    for index, row in enumerate(rows, 1):
        lines.append(
            f"{index}. {row.code} {row.name} | 最新价 {float(row.latest_price):.2f} | "
            f"当前盈亏 {float(row.pnl_pct):+.2f}%"
        )
        if row.target_price or row.stop_loss:
            lines.append(
                f"   预期止盈止损: {float(row.target_price):.2f} / {float(row.stop_loss):.2f}"
            )
        lines.append(f"   量化因子: {str(row.factor_text).replace('量化因子:', '').strip()}")
        lines.append(f"   基本面判断: {str(row.fundamental_text).replace('基本面:', '').strip()}")
        lines.append(f"   技术面判断: {str(row.tech_text).replace('技术面:', '').strip()}")
        lines.append(f"   资金面观察: {str(row.fund_text).replace('资金面:', '').strip()}")
        lines.append(f"   情绪面观察: {str(row.emotion_text).replace('情绪面:', '').strip()}")
        lines.append("-" * 22)
    return "\n".join(lines)


def format_decision_section(rows: List[DecisionReportRow]) -> str:
    """格式化决策分析区块（移动端友好版）"""
    if not rows:
        return "【决策分析】\n当前空仓，无需加仓或清仓"

    lines: List[str] = ["【决策分析】"]
    for index, row in enumerate(rows, 1):
        reasons = row.reasons or ["暂无额外说明"]
        lines.append(f"{index}. {row.code} {row.name} | 当前建议: {row.action}")
        for reason_index, reason in enumerate(reasons, 1):
            lines.append(f"   原因{reason_index}: {str(reason).strip()}")
        lines.append("-" * 22)
    return "\n".join(lines)


def format_signal_section(etf_recs: List[SignalRecommendRow], stock_recs: List[SignalRecommendRow]) -> str:
    """格式化信号推荐区块（移动端友好版）"""
    lines = ["【信号推荐】"]

    all_recs = list(stock_recs or []) + list(etf_recs or [])
    if not all_recs:
        lines.append("暂无新增信号")
        return "\n".join(lines)

    for index, rec in enumerate(all_recs, 1):
        view_text = str(rec.signal or "观望")
        order_book_text = str(rec.order_book_text or "暂无")
        if order_book_text.startswith("盘口"):
            order_book_text = order_book_text[2:]
        focus = "重点关注" if rec.dual_signal else "常规跟踪"
        lines.append(f"{index}. {rec.code} {rec.name} | {view_text} | 盘口{order_book_text} | {focus}")
        lines.append(f"   最新价格: {float(rec.price):.2f} | 涨跌: {float(rec.change_pct):+.2f}%")
        if rec.target and rec.stop_loss:
            lines.append(f"   止盈止损: 止盈 {float(rec.target):.2f} / 止损 {float(rec.stop_loss):.2f}")
        else:
            lines.append("   止盈止损: 暂未设置")
        reason_text = str(rec.reason or "暂无特别说明").replace("\n", " ").strip()
        lines.append(f"   看法说明: {reason_text}")
        lines.append("-" * 22)

    return "\n".join(lines)


def format_review_section(stats: Dict, trades: List[ReviewTradeRow], proxy_diff_rows: Optional[List[ProxyDiffRow]] = None) -> str:
    """格式化回测复盘区块"""
    lines = ["【回测复盘】"]
    lines.append(
        f"卖出统计: 交易{int(stats.get('total_trades', 0))}次 | 胜率{float(stats.get('win_rate', 0.0)):.1f}% | "
        f"总收益{float(stats.get('total_pnl', 0.0)):.2f} | 平均收益{float(stats.get('avg_pnl', 0.0)):.2f}"
    )
    if trades:
        lines.append("最近成交")
        for trade in trades[:5]:
            lines.append(f"{trade.date} {trade.code} {trade.direction} @ {float(trade.price):.2f} PnL {float(trade.pnl):.2f}")
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
    lines = ["【交易时间线】"]
    for row in rows:
        event_label = event_type_labels.get(row.event_type, row.event_type or "事件")
        parts = [f"{row.date} {row.code} {row.name}", event_label]
        if row.signal_type:
            parts.append(row.signal_type)
        if row.price:
            parts.append(f"价格{row.price:.2f}")
        if row.target_price or row.stop_loss:
            parts.append(f"止盈/止损{row.target_price:.2f}/{row.stop_loss:.2f}")
        if row.quantity:
            parts.append(f"数量{int(row.quantity)}")
        if row.status:
            parts.append(f"状态{to_status_label(row.status)}")
        if row.pnl:
            parts.append(f"收益{row.pnl:.2f}")
        if row.pnl_pct:
            parts.append(f"收益率{row.pnl_pct:+.2f}%")
        lines.append(" | ".join(parts))
        if row.reason:
            lines.append(f"理由: {row.reason}")
    return "\n".join(lines)


def format_trade_lifecycle_section(rows: List[TradeLifecycleRow]) -> str:
    """格式化交易摘要区块"""
    if not rows:
        return "【交易摘要】\n暂无持仓或历史交易"

    lines = ["【交易摘要】"]
    for row in rows:
        lines.append(
            f"{row.code} {row.name} | 开仓成本{row.open_cost:.2f} | 持仓{int(row.holding_quantity)} | 最新价{row.latest_price:.2f}"
        )
        lines.append(
            f"浮盈{row.floating_pnl:.2f}({row.floating_pnl_pct:+.2f}%) | 已实现{row.realized_pnl:.2f} | 总收益{row.total_pnl:.2f}({row.total_pnl_pct:+.2f}%)"
        )
        lines.append("-" * 24)
    return "\n".join(lines)
