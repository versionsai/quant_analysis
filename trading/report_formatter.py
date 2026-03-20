# -*- coding: utf-8 -*-
"""
推送报告格式化模块
统一管理综合报告的结构与文案格式
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


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
