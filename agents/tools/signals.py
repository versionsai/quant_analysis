# -*- coding: utf-8 -*-
"""
量化信号工具
获取量化策略产生的交易信号
"""
from datetime import datetime
import os
from langchain_core.tools import tool

from agents.skills import get_skills_manager, load_skills
from data.recommend_db import get_db
from trading.realtime_monitor import RealtimeMonitor
from utils.logger import get_logger

logger = get_logger(__name__)

ETF_LIKE_PREFIXES = ("15", "16", "50", "51", "52", "56", "58", "159", "501")


def _load_signal_skill_config() -> tuple:
    """加载 signal skill 配置，并映射到实时监控参数。"""
    try:
        load_skills()
        manager = get_skills_manager()
        skill = manager.get_skill("signal")
        params = skill.get("params", {}) if skill else {}
        target_loss = skill.get("target_loss", {}) if skill else {}
        pool_cfg = skill.get("pool", {}) if skill else {}

        strategy_overrides = {
            "lookback": int(params.get("lookback", 20)),
            "macd_fast": int(params.get("macd_fast", 12)),
            "macd_slow": int(params.get("macd_slow", 26)),
            "macd_signal": int(params.get("macd_signal", 9)),
        }
        risk_overrides = {}
        if "profit_target_pct" in target_loss:
            risk_overrides["take_profit"] = float(target_loss.get("profit_target_pct", 5.0)) / 100.0
        if "stop_loss_pct" in target_loss:
            risk_overrides["stop_loss"] = -abs(float(target_loss.get("stop_loss_pct", 3.0)) / 100.0)

        return strategy_overrides, risk_overrides, pool_cfg
    except Exception as e:
        logger.warning(f"读取 signal skill 配置失败，使用默认参数: {e}")
        return {}, {}, {}


def _is_etf_like(code: str) -> bool:
    """基于代码前缀判断是否为 ETF/LOF 类标的。"""
    normalized = str(code or "").strip().zfill(6)
    return normalized.startswith(ETF_LIKE_PREFIXES)


def _merge_pool_items(base_items: list, extra_items: list, limit: int) -> list:
    """合并股票池条目并按代码去重。"""
    merged = []
    seen = set()
    for item in (base_items or []) + (extra_items or []):
        code = str(item.get("code", "")).strip().zfill(6)
        if not code or code in seen:
            continue
        seen.add(code)
        merged.append({"code": code, "name": str(item.get("name", "")).strip()})
        if limit > 0 and len(merged) >= limit:
            break
    return merged


def _load_dynamic_pool_items(pool_cfg: dict) -> tuple:
    """按配置从动态池、当日荐股、当前持仓中解析扫描池。"""
    if str(pool_cfg.get("mode", "dynamic")).lower() != "dynamic":
        return [], []

    etf_limit = int(pool_cfg.get("etf_limit", 20))
    stock_limit = int(pool_cfg.get("stock_limit", 50))
    include_today_recommends = bool(pool_cfg.get("include_today_recommends", True))
    include_current_holdings = bool(pool_cfg.get("include_current_holdings", True))
    include_signal_pool = bool(pool_cfg.get("include_signal_pool", True))

    etf_items = []
    stock_items = []
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")

    if include_today_recommends or include_current_holdings or include_signal_pool:
        try:
            db = get_db(db_path)

            if include_today_recommends:
                today = datetime.now().strftime("%Y-%m-%d")
                for rec in db.get_recommends_by_date(today):
                    item = {"code": rec.code, "name": rec.name}
                    if _is_etf_like(rec.code):
                        etf_items.append(item)
                    else:
                        stock_items.append(item)

            if include_current_holdings:
                for holding in db.get_holdings():
                    item = {"code": holding.get("code", ""), "name": holding.get("name", "")}
                    if _is_etf_like(holding.get("code", "")):
                        etf_items.append(item)
                    else:
                        stock_items.append(item)

            if include_signal_pool:
                for signal_item in db.get_signal_pool(limit=max(etf_limit, stock_limit, 50)):
                    item = {"code": signal_item.get("code", ""), "name": signal_item.get("name", "")}
                    if _is_etf_like(signal_item.get("code", "")):
                        etf_items.append(item)
                    else:
                        stock_items.append(item)
        except Exception as e:
            logger.warning(f"读取动态信号池失败，回退默认扫描池: {e}")

    return etf_items[:etf_limit], stock_items[:stock_limit]


@tool
def check_quant_signals() -> str:
    """
    获取量化策略产生的最新交易信号，包括ETF和A股的买入/卖出/观望信号。

    Returns:
        str: 量化信号报告，包含所有扫描股票的信号详情
    """
    try:
        logger.info("开始获取量化信号...")
        strategy_overrides, risk_overrides, pool_cfg = _load_signal_skill_config()

        monitor = RealtimeMonitor(
            etf_count=5,
            stock_count=5,
            strategy_overrides=strategy_overrides,
            risk_overrides=risk_overrides,
        )
        if not bool(pool_cfg.get("dynamic_pool_enabled", True)):
            monitor.etf_pool = []
            monitor.stock_pool = []
        etf_extra_items, stock_extra_items = _load_dynamic_pool_items(pool_cfg)
        monitor.etf_pool = _merge_pool_items(
            monitor.etf_pool,
            etf_extra_items,
            int(pool_cfg.get("etf_limit", 20)),
        )
        monitor.stock_pool = _merge_pool_items(
            monitor.stock_pool,
            stock_extra_items,
            int(pool_cfg.get("stock_limit", 50)),
        )
        results = monitor.scan_market()

        result = "【量化信号报告】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        if strategy_overrides or risk_overrides:
            result += (
                f"参数: MACD({strategy_overrides.get('macd_fast', 12)},"
                f"{strategy_overrides.get('macd_slow', 26)},"
                f"{strategy_overrides.get('macd_signal', 9)}) "
                f"Lookback={strategy_overrides.get('lookback', 20)} "
                f"止盈/止损={risk_overrides.get('take_profit', 0.15):.0%}/"
                f"{abs(risk_overrides.get('stop_loss', -0.05)):.0%}\n\n"
            )
        if pool_cfg:
            result += (
                "池来源: dynamic_pool="
                f"{bool(pool_cfg.get('dynamic_pool_enabled', True))}, "
                "today_recommends="
                f"{bool(pool_cfg.get('include_today_recommends', True))}, "
                "current_holdings="
                f"{bool(pool_cfg.get('include_current_holdings', True))}\n\n"
            )

        etf_signals = results.get("etf", [])
        stock_signals = results.get("stock", [])

        buy_etf = [s for s in etf_signals if s.signal_type == "买入"]
        buy_stock = [s for s in stock_signals if s.signal_type == "买入"]

        result += "【ETF/LOF 信号】\n"
        if etf_signals:
            for s in etf_signals[:5]:
                signal_emoji = "📈" if s.signal_type == "买入" else ("📉" if s.signal_type == "卖出" else "⏸️")
                result += f"{signal_emoji} {s.code} {s.name}\n"
                result += f"   价格: {s.price:.2f} 涨跌幅: {s.change_pct:+.2f}%\n"
                result += f"   信号: {s.signal_type}\n"
                if s.target_price and s.stop_loss:
                    profit_pct = (s.target_price / s.price - 1) * 100
                    loss_pct = (s.stop_loss / s.price - 1) * 100
                    result += f"   目标: {s.target_price:.2f}(+{profit_pct:.1f}%) "
                    result += f"止损: {s.stop_loss:.2f}({loss_pct:.1f}%)\n"
                result += f"   理由: {s.reason}\n\n"
        else:
            result += "  暂无数据\n"
            result += "\n"

        result += "【A股信号】\n"
        if stock_signals:
            for s in stock_signals[:5]:
                signal_emoji = "📈" if s.signal_type == "买入" else ("📉" if s.signal_type == "卖出" else "⏸️")
                result += f"{signal_emoji} {s.code} {s.name}\n"
                result += f"   价格: {s.price:.2f} 涨跌幅: {s.change_pct:+.2f}%\n"
                result += f"   信号: {s.signal_type}\n"
                if s.target_price and s.stop_loss:
                    profit_pct = (s.target_price / s.price - 1) * 100
                    loss_pct = (s.stop_loss / s.price - 1) * 100
                    result += f"   目标: {s.target_price:.2f}(+{profit_pct:.1f}%) "
                    result += f"止损: {s.stop_loss:.2f}({loss_pct:.1f}%)\n"
                result += f"   理由: {s.reason}\n\n"
        else:
            result += "  暂无数据\n"
            result += "\n"

        result += "【信号汇总】\n"
        result += f"  ETF买入信号: {len(buy_etf)}只\n"
        result += f"  A股买入信号: {len(buy_stock)}只\n"

        if buy_etf or buy_stock:
            result += "\n【建议操作】\n"
            if buy_etf:
                result += "ETF推荐:\n"
                for s in buy_etf[:2]:
                    result += f"  ✅ {s.code} {s.name} @ {s.price:.2f}\n"
            if buy_stock:
                result += "A股推荐:\n"
                for s in buy_stock[:2]:
                    result += f"  ✅ {s.code} {s.name} @ {s.price:.2f}\n"
        else:
            result += "\n【建议操作】: 暂无买入信号，建议观望\n"

        return result

    except Exception as e:
        logger.error(f"获取量化信号失败: {e}")
        return f"获取量化信号失败: {str(e)}"
