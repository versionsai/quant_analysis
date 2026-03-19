# -*- coding: utf-8 -*-
"""
TradingAgents 工具封装

将 TauricResearch/TradingAgents 的分析能力暴露为 LangChain Tool，供 DeepAgents 调用。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from agents.tradingagents_bridge import run_tradingagents
from utils.logger import get_logger

logger = get_logger(__name__)


def _today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _fetch_us_realtime(symbols: list) -> list:
    """
    从新浪财经获取美股/ETF实时行情（不需要API key）
    Returns: list of dicts with code, name, price, change_pct, change_amt, high, low
    """
    try:
        import requests
        syms = ','.join(f'gb_{s.lower()}' for s in symbols)
        headers = {
            'Referer': 'https://finance.sina.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        }
        r = requests.get(f'https://hq.sinajs.cn/list={syms}', headers=headers, timeout=10)
        r.encoding = 'gbk'
        results = []
        for line in r.text.strip().split('\n'):
            if 'hq_str_gb_' not in line:
                continue
            code = line.split('hq_str_gb_')[1].split('=')[0].strip()
            parts = line.split('"')[1].split(',')
            if len(parts) < 9:
                continue
            try:
                results.append({
                    'code': code.upper(),
                    'name': parts[0],
                    'price': float(parts[1]),
                    'change_pct': float(parts[2]),
                    'change_amt': float(parts[4]),
                    'open': float(parts[5]),
                    'prev_close': float(parts[6]),
                    'high': float(parts[7]),
                    'low': float(parts[8]),
                })
            except (ValueError, IndexError):
                continue
        return results
    except Exception as e:
        logger.warning(f"获取美股实时数据失败: {e}")
        return []


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


def _us_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s in ("SPY", "QQQ", "IWM", "DIA", "AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "AMD", "NFLX"):
        return s
    if s.endswith((".US", ".O")):
        return s
    return f"{s}.US"


def _format_us_analysis(payload: Dict[str, Any]) -> str:
    state = payload.get("state") or {}
    decision = payload.get("decision", "")

    lines = []
    lines.append("【美股 TradingAgents 分析】")
    lines.append(f"标的: {payload.get('ticker', '')} 日期: {payload.get('trade_date', '')}")
    lines.append("")

    mr = state.get("market_report", "")
    nr = state.get("news_report", "")
    sr = state.get("sentiment_report", "")
    fr = state.get("fundamentals_report", "")
    rr = state.get("risk_report", "")

    if mr:
        lines.append(f"【大盘】{str(mr).strip()}")
    if sr:
        lines.append(f"【情绪】{str(sr).strip()}")
    if nr:
        lines.append(f"【新闻】{str(nr).strip()[:300]}")
    if fr:
        lines.append(f"【基本面】{str(fr).strip()[:300]}")
    if rr:
        lines.append(f"【风险】{str(rr).strip()[:300]}")
    if decision:
        lines.append(f"【结论】{str(decision).strip()}")

    return "\n".join(lines).strip()


@tool
def ta_analyze_us_market(symbols: str = "SPY,QQQ") -> str:
    """
    获取美股大盘实时行情（来自新浪财经，不依赖外部API），并结合TradingAgents进行深度分析。

    支持的美股ETF代码：SPY(标普500)、QQQ(纳斯达克100)、IWM(小盘股)、DIA(道琼斯)、
    NVDA/AAPL/MSFT/GOOGL/AMZN/META(科技巨头)。

    Args:
        symbols: 美股代码，多个用逗号分隔，默认 "SPY,QQQ"

    Returns:
        str: 美股行情报告 + TradingAgents深度分析
    """
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        symbol_list = ["SPY", "QQQ"]

    # Step 1: 从新浪获取实时行情（快速、准确）
    realtime_data = _fetch_us_realtime(symbol_list[:4])

    # Step 2: 构建实时行情报告（不依赖LLM计算）
    lines = ["【美股实时行情】(数据来源: 新浪财经)"]

    if realtime_data:
        for item in realtime_data:
            pct = item["change_pct"]
            trend = "📈" if pct > 0 else "📉" if pct < 0 else "➡️"
            lines.append(
                f"{trend} {item['code']}({item['name']}): "
                f"现价={item['price']:.2f} "
                f"涨跌={item['change_amt']:+.2f}({pct:+.2f}%) "
                f"今开={item['open']:.2f} "
                f"昨收={item['prev_close']:.2f} "
                f"最高={item['high']:.2f} 最低={item['low']:.2f}"
            )
    else:
        lines.append("(实时行情获取失败)")

    # Step 3: 基于实时涨跌，生成大盘情绪评估
    if realtime_data:
        avg_pct = sum(d["change_pct"] for d in realtime_data) / len(realtime_data)
        if avg_pct < -1.5:
            sentiment = "美股大跌，市场恐慌情绪蔓延，A股明日承压，建议谨慎。"
            cn_impact = "利空A股，明日A股大概率低开，关注防御性板块。"
        elif avg_pct < -0.5:
            sentiment = "美股小幅下跌，市场偏谨慎，A股明日需观察开盘情况。"
            cn_impact = "偏利空，建议控制仓位，不盲目追高。"
        elif avg_pct > 1.5:
            sentiment = "美股大涨，市场风险偏好提升，A股明日大概率高开。"
            cn_impact = "利好A股，可积极关注近期强势板块。"
        elif avg_pct > 0.5:
            sentiment = "美股小幅上涨，市场情绪偏暖，A股明日有望跟涨。"
            cn_impact = "偏利好，可适度加仓热门标的。"
        else:
            sentiment = "美股基本持平，市场观望情绪浓厚，等待方向指引。"
            cn_impact = "中性，A股维持震荡概率大，轻仓观望为主。"
        lines.append(f"\n【大盘情绪】{sentiment}")
        lines.append(f"【对A股影响】{cn_impact}")

    # Step 4: TradingAgents 深度分析（可选，若不被限流）
    lines.append("\n【TradingAgents 深度分析】")
    try:
        from agents.tradingagents_bridge import run_tradingagents, _normalize_analysts
        ta_results = []
        for sym in symbol_list[:2]:
            try:
                payload = run_tradingagents(
                    ticker_or_symbol=sym,
                    trade_date=date.today().strftime("%Y-%m-%d"),
                    selected_analysts=["market_analyst", "news_analyst", "sentiment_analyst"],
                )
                ta_results.append(_format_us_analysis(payload))
            except Exception as e:
                ta_results.append(f"【{sym}】分析失败: {str(e)}")
        lines.append("\n".join(ta_results))
    except ImportError:
        lines.append("TradingAgents 未安装，跳过深度分析。")
    except Exception as e:
        lines.append(f"TradingAgents 调用失败: {str(e)}")

    return "\n".join(lines)


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

