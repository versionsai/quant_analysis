# -*- coding: utf-8 -*-
"""
多Agent辩论系统模块
"""
from .optimist_agent import OptimistAgent
from .pessimist_agent import PessimistAgent
from .risk_agent import RiskAgent
from .judge_agent import JudgeAgent

__all__ = [
    "OptimistAgent",
    "PessimistAgent",
    "RiskAgent",
    "JudgeAgent",
]
