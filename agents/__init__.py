# -*- coding: utf-8 -*-
"""
量化交易 Agent 模块
基于 DeepAgents 框架构建
"""
from .quant_agent import QuantAgent, get_quant_agent, init_quant_agent
from .strategy_tuner import StrategyTuningAdvisor, save_tuning_review
from .strategy_experimenter import (
    rank_experiment_candidates,
    run_strategy_experiments,
    save_best_config_snapshot,
    save_experiment_report,
)
from .llm import SiliconFlowLLM, get_llm, init_llm, set_api_key
from .skills import SkillsManager, get_skills_manager, load_skills
from .tools import (
    get_market_sentiment,
    analyze_portfolio,
    check_quant_signals,
    push_report,
    get_global_finance_news,
    get_policy_news,
    get_holding_announcements,
    push_news_report,
    analyze_stock,
)

__all__ = [
    "QuantAgent",
    "get_quant_agent",
    "init_quant_agent",
    "StrategyTuningAdvisor",
    "save_tuning_review",
    "run_strategy_experiments",
    "rank_experiment_candidates",
    "save_best_config_snapshot",
    "save_experiment_report",
    "SiliconFlowLLM",
    "get_llm",
    "init_llm",
    "set_api_key",
    "SkillsManager",
    "get_skills_manager",
    "load_skills",
    "get_market_sentiment",
    "analyze_portfolio",
    "check_quant_signals",
    "push_report",
    "get_global_finance_news",
    "get_policy_news",
    "get_holding_announcements",
    "push_news_report",
    "analyze_stock",
]
