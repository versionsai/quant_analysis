# -*- coding: utf-8 -*-
"""
TradingAgents 桥接模块

以“可选依赖”的方式集成 TauricResearch/TradingAgents，用于为本项目的 AI Agent 提供更丰富的:
- 单股深度分析（技术/基本面/新闻/情绪/风险）
- 大盘情绪与观点（用指数 ticker 代替）
"""

from __future__ import annotations

import os
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple

from utils.logger import get_logger

logger = get_logger(__name__)

_graph_instance: Any = None
_graph_config_sig: Tuple = ()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "")
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _build_config_from_env() -> Dict[str, Any]:
    """
    由环境变量构建 TradingAgents 配置（覆盖默认配置）。

    关键环境变量:
    - TRADINGAGENTS_LLM_PROVIDER: openai/openrouter/...
    - TRADINGAGENTS_DEEP_MODEL, TRADINGAGENTS_QUICK_MODEL
    - TRADINGAGENTS_MAX_DEBATE_ROUNDS
    - TRADINGAGENTS_ONLINE_TOOLS: true/false
    """
    from tradingagents.default_config import DEFAULT_CONFIG

    cfg = deepcopy(DEFAULT_CONFIG)

    # 约束：项目内固定使用 OpenAI-compatible 模式（通过 SiliconFlow 提供的 OpenAI 兼容接口）。
    cfg["llm_provider"] = "openai"

    # TradingAgents 的 OpenAI 客户端通常读取 OPENAI_API_KEY。
    # 为了复用现有 SiliconFlow 配置：若 OPENAI_API_KEY 未设置，则回退使用 SILICONFLOW_API_KEY。
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        sf_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
        if sf_key:
            os.environ["OPENAI_API_KEY"] = sf_key

    deep_model = os.environ.get("TRADINGAGENTS_DEEP_MODEL", "").strip()
    if deep_model:
        cfg["deep_think_llm"] = deep_model

    quick_model = os.environ.get("TRADINGAGENTS_QUICK_MODEL", "").strip()
    if quick_model:
        cfg["quick_think_llm"] = quick_model

    backend_url = os.environ.get("TRADINGAGENTS_BACKEND_URL", "").strip() or "https://api.siliconflow.cn/v1"
    cfg["backend_url"] = backend_url

    max_rounds = os.environ.get("TRADINGAGENTS_MAX_DEBATE_ROUNDS", "").strip()
    if max_rounds:
        try:
            cfg["max_debate_rounds"] = int(max_rounds)
        except Exception:
            logger.warning(f"TRADINGAGENTS_MAX_DEBATE_ROUNDS 无法解析为 int: {max_rounds}")

    cfg["online_tools"] = _env_bool("TRADINGAGENTS_ONLINE_TOOLS", default=cfg.get("online_tools", True))

    return cfg


def _config_signature(cfg: Dict[str, Any]) -> Tuple:
    return (
        cfg.get("llm_provider", ""),
        cfg.get("deep_think_llm", ""),
        cfg.get("quick_think_llm", ""),
        cfg.get("backend_url", ""),
        cfg.get("max_debate_rounds", 1),
        bool(cfg.get("online_tools", True)),
    )


def get_tradingagents_graph(force_reload: bool = False):
    """
    获取 TradingAgentsGraph 单例。

    如果 TradingAgents 未安装，会抛出 ImportError，由上层工具捕获并给出安装提示。
    """
    global _graph_instance, _graph_config_sig

    cfg = _build_config_from_env()
    sig = _config_signature(cfg)

    if force_reload or _graph_instance is None or _graph_config_sig != sig:
        from tradingagents.graph.trading_graph import TradingAgentsGraph

        logger.info(f"初始化 TradingAgentsGraph: provider={sig[0]} deep={sig[1]} quick={sig[2]} rounds={sig[3]}")
        _graph_instance = TradingAgentsGraph(cfg)
        _graph_config_sig = sig

    return _graph_instance


def normalize_cn_ticker(symbol: str) -> str:
    """
    将 A 股 6 位代码转换为 TradingAgents 常用数据源更兼容的 ticker（偏向 yfinance 习惯）。

    约定:
    - 6/5/9 开头 → 上交所 .SS
    - 0/3/2/1 开头 → 深交所 .SZ
    - 已包含后缀（.SS/.SZ/.HK/.US）则原样返回
    """
    s = str(symbol).strip().upper()
    if s.endswith((".SS", ".SZ", ".HK", ".US")):
        return s

    if s.isdigit() and len(s) == 6:
        if s.startswith(("6", "5", "9")):
            return f"{s}.SS"
        return f"{s}.SZ"

    return s


def run_tradingagents(
    ticker_or_symbol: str,
    trade_date: str,
    selected_analysts: Optional[list] = None,
) -> Dict[str, Any]:
    """
    运行 TradingAgents pipeline，返回最终 state（dict）。
    """
    ta = get_tradingagents_graph()
    ticker = normalize_cn_ticker(ticker_or_symbol)
    _final_state, decision = ta.propagate(ticker, trade_date, selected_analysts=selected_analysts)
    if isinstance(_final_state, dict):
        _final_state.setdefault("final_decision", decision)
    return {"ticker": ticker, "trade_date": trade_date, "state": _final_state, "decision": decision}
