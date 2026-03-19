# -*- coding: utf-8 -*-
"""
TradingAgents 工具封装

将 TauricResearch/TradingAgents 的分析能力暴露为 LangChain Tool，供 DeepAgents 调用。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from agents.tradingagents_bridge import run_tradingagents
from utils.logger import get_logger

logger = get_logger(__name__)


def _today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_get(d: Any, key: str, default: Any = "") -> Any:
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def _format_tradingagents_result(payload: Dict[str, Any]) -> str:
    ticker = payload.get("ticker", "")
    trade_date = payload.get("trade_date", "")
    state = payload.get("state") or {}
    decision = payload.get("decision", "")

    market_report = _safe_get(state, "market_report", "")
    sentiment_report = _safe_get(state, "sentiment_report", "")
    news_report = _safe_get(state, "news_report", "")
    fundamentals_report = _safe_get(state, "fundamentals_report", "")
    risk_report = _safe_get(state, "risk_report", "")
    technical_report = _safe_get(state, "technical_report", "")

    lines = []
    lines.append(f"【TradingAgents 分析】{ticker}")
    lines.append(f"日期: {trade_date}")
    lines.append("")

    if market_report:
        lines.append("【大盘/宏观】")
        lines.append(str(market_report).strip())
        lines.append("")

    if sentiment_report:
        lines.append("【情绪/舆情】")
        lines.append(str(sentiment_report).strip())
        lines.append("")

    if news_report:
        lines.append("【新闻/事件】")
        lines.append(str(news_report).strip())
        lines.append("")

    if fundamentals_report:
        lines.append("【基本面】")
        lines.append(str(fundamentals_report).strip())
        lines.append("")

    if technical_report:
        lines.append("【技术面】")
        lines.append(str(technical_report).strip())
        lines.append("")

    if risk_report:
        lines.append("【风险】")
        lines.append(str(risk_report).strip())
        lines.append("")

    lines.append("【结论】")
    lines.append(str(decision).strip() if decision else "无决策输出")

    return "\n".join(lines).strip() + "\n"


@tool
def ta_analyze_stock(symbol: str, trade_date: str = "") -> str:
    """
    使用 TradingAgents 对单只股票/ETF/指数进行多维度分析（技术/基本面/新闻/情绪/风险）并给出结论。

    Args:
        symbol: 股票代码或 ticker，例如 "600036"、"000001"、"600036.SS"、"AAPL"
        trade_date: 交易日期，格式建议 YYYY-MM-DD；为空则默认今天

    Returns:
        str: TradingAgents 分析报告
    """
    try:
        d = trade_date.strip() if trade_date else _today_iso()
        payload = run_tradingagents(
            ticker_or_symbol=symbol,
            trade_date=d,
            selected_analysts=["market_analyst", "news_analyst", "fundamentals_analyst", "technical_analyst", "risk_manager"],
        )
        return _format_tradingagents_result(payload)
    except ImportError:
        return "TradingAgents 未安装：请先执行 pip install tradingagents，然后重试 ta_analyze_stock。"
    except Exception as e:
        logger.error(f"TradingAgents 股票分析失败 {symbol}: {e}")
        return f"TradingAgents 股票分析失败: {str(e)}"


@tool
def ta_market_sentiment(index_symbol: str = "000001", trade_date: str = "") -> str:
    """
    使用 TradingAgents 对大盘/指数进行情绪与观点分析（用指数 ticker 代替大盘）。

    Args:
        index_symbol: 指数代码或 ticker，例如 "000001"(上证指数)、"399001"(深证成指)、"000300"(沪深300)
        trade_date: 日期，格式建议 YYYY-MM-DD；为空则默认今天

    Returns:
        str: TradingAgents 大盘情绪分析报告
    """
    try:
        d = trade_date.strip() if trade_date else _today_iso()
        payload = run_tradingagents(
            ticker_or_symbol=index_symbol,
            trade_date=d,
            selected_analysts=["market_analyst", "news_analyst", "sentiment_analyst", "risk_manager"],
        )
        return _format_tradingagents_result(payload)
    except ImportError:
        return "TradingAgents 未安装：请先执行 pip install tradingagents，然后重试 ta_market_sentiment。"
    except Exception as e:
        logger.error(f"TradingAgents 大盘情绪分析失败 {index_symbol}: {e}")
        return f"TradingAgents 大盘情绪分析失败: {str(e)}"

