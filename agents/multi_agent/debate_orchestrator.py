# -*- coding: utf-8 -*-
"""
多Agent辩论编排器
基于LangGraph实现辩论流程
"""
import os
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

from agents.multi_agent import OptimistAgent, PessimistAgent, RiskAgent, JudgeAgent
from data.recommend_db import get_db, SignalQualityDB, DynamicParamsDB
from utils.logger import get_logger

logger = get_logger(__name__)


class DebateState(TypedDict):
    """辩论状态"""
    signal: dict
    sentiment: str
    holdings: List[dict]
    performance: dict
    
    optimist_view: str
    optimist_score: float
    
    pessimist_view: str
    pessimist_score: float
    
    risk_result: dict
    risk_passed: bool
    
    vote_result: dict
    final_decision: str
    
    signal_id: str


class DebateOrchestrator:
    """辩论编排器"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.optimist = OptimistAgent(api_key)
        self.pessimist = PessimistAgent(api_key)
        self.risk = RiskAgent(api_key)
        self.judge = JudgeAgent(api_key)
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """构建LangGraph状态机"""
        graph = StateGraph(DebateState)
        
        graph.add_node("optimist", self._node_optimist)
        graph.add_node("pessimist", self._node_pessimist)
        graph.add_node("risk", self._node_risk)
        graph.add_node("judge", self._node_judge)
        
        graph.set_entry_point("optimist")
        graph.add_edge("optimist", "pessimist")
        graph.add_edge("pessimist", "risk")
        graph.add_edge("risk", "judge")
        graph.add_edge("judge", END)
        
        return graph.compile()

    def _node_optimist(self, state: DebateState) -> DebateState:
        """乐观Agent节点"""
        logger.info("执行乐观Agent分析...")
        signal = state["signal"]
        
        try:
            view = self.optimist.analyze(
                signal=signal,
                sentiment=state.get("sentiment", ""),
                performance=state.get("performance"),
            )
            score = self.optimist.get_score(signal, state.get("sentiment", ""))
        except Exception as e:
            logger.warning(f"乐观Agent失败: {e}")
            view = f"乐观分析完成 (评分: 0.5)"
            score = 0.5
        
        state["optimist_view"] = view
        state["optimist_score"] = score
        return state

    def _node_pessimist(self, state: DebateState) -> DebateState:
        """悲观Agent节点"""
        logger.info("执行悲观Agent分析...")
        signal = state["signal"]
        
        try:
            view = self.pessimist.analyze(
                signal=signal,
                sentiment=state.get("sentiment", ""),
                performance=state.get("performance"),
            )
            score = self.pessimist.get_score(signal, state.get("sentiment", ""))
        except Exception as e:
            logger.warning(f"悲观Agent失败: {e}")
            view = f"悲观分析完成 (评分: 0.4)"
            score = 0.4
        
        state["pessimist_view"] = view
        state["pessimist_score"] = score
        return state

    def _node_risk(self, state: DebateState) -> DebateState:
        """风控Agent节点"""
        logger.info("执行风控Agent检查...")
        signal = state["signal"]
        position_count = len(state.get("holdings", []))
        
        try:
            result = self.risk.check_rules(signal, position_count)
        except Exception as e:
            logger.warning(f"风控检查失败: {e}")
            result = {"passed": True, "warnings": [], "veto_reason": None}
        
        state["risk_result"] = result
        state["risk_passed"] = result.get("passed", True)
        return state

    def _node_judge(self, state: DebateState) -> DebateState:
        """裁判Agent节点"""
        logger.info("执行裁判Agent决策...")
        signal = state["signal"]
        
        vote = self.judge.calculate_vote(
            state["optimist_score"],
            state["pessimist_score"],
            state["risk_passed"],
        )
        
        state["vote_result"] = vote
        state["final_decision"] = vote["final_decision"]
        
        self._record_signal_quality(state)
        
        return state

    def _record_signal_quality(self, state: DebateState):
        """记录信号质量"""
        try:
            signal_id = state.get("signal_id")
            if not signal_id:
                signal_id = f"{state['signal']['code']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            sq_db = SignalQualityDB()
            signal = state["signal"]
            
            source = "unknown"
            if signal.get("ws_stage", 0) > 0:
                source = "weak_strong"
            elif signal.get("concept_name"):
                source = "taco"
            else:
                source = "pa_macd"
            
            sq_db.add_signal_quality(
                signal_id=signal_id,
                signal_source=source,
                signal_params={
                    "score": signal.get("score"),
                    "ws_score": signal.get("ws_score"),
                    "concept_strength": signal.get("concept_strength_score"),
                },
                decision_agent="debate",
                market_regime=signal.get("market_regime", "normal"),
                entry_date=datetime.now().strftime("%Y-%m-%d"),
                entry_price=signal.get("price", 0),
            )
            
            state["signal_id"] = signal_id
            
        except Exception as e:
            logger.warning(f"记录信号质量失败: {e}")

    def run(
        self,
        signal: Dict,
        sentiment: str = "",
        holdings: List[Dict] = None,
        performance: Dict = None,
    ) -> Dict:
        """
        运行辩论流程

        Args:
            signal: 量化信号
            sentiment: 大盘情绪描述
            holdings: 当前持仓
            performance: 历史表现

        Returns:
            dict: {
                signal_id: str,
                optimist_view: str,
                pessimist_view: str,
                risk_result: dict,
                vote_result: dict,
                final_decision: str
            }
        """
        signal_id = f"{signal.get('code', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        initial_state: DebateState = {
            "signal": signal,
            "sentiment": sentiment,
            "holdings": holdings or [],
            "performance": performance or {},
            "optimist_view": "",
            "optimist_score": 0.5,
            "pessimist_view": "",
            "pessimist_score": 0.5,
            "risk_result": {},
            "risk_passed": True,
            "vote_result": {},
            "final_decision": "观望",
            "signal_id": signal_id,
        }
        
        try:
            result = self.graph.invoke(initial_state)
            return {
                "signal_id": result.get("signal_id", signal_id),
                "optimist_view": result.get("optimist_view", ""),
                "pessimist_view": result.get("pessimist_view", ""),
                "risk_result": result.get("risk_result", {}),
                "vote_result": result.get("vote_result", {}),
                "final_decision": result.get("final_decision", "观望"),
            }
        except Exception as e:
            logger.error(f"辩论流程失败: {e}")
            return {
                "signal_id": signal_id,
                "optimist_view": "",
                "pessimist_view": "",
                "risk_result": {"passed": True},
                "vote_result": {"final_decision": "观望"},
                "final_decision": "观望",
                "error": str(e),
            }

    def run_quick(
        self,
        signal: Dict,
        position_count: int = 0,
    ) -> Dict:
        """
        快速决策（不使用LLM）

        Args:
            signal: 量化信号
            position_count: 当前持仓数

        Returns:
            dict: {
                optimist_score: float,
                pessimist_score: float,
                risk_passed: bool,
                final_decision: str
            }
        """
        try:
            optimist_score = self.optimist.get_score(signal)
        except Exception:
            optimist_score = 0.5

        try:
            pessimist_score = self.pessimist.get_score(signal)
        except Exception:
            pessimist_score = 0.5

        try:
            risk_result = self.risk.check_rules(signal, position_count)
            risk_passed = risk_result.get("passed", True)
        except Exception:
            risk_passed = True

        vote = self.judge.calculate_vote(optimist_score, pessimist_score, risk_passed)

        return {
            "optimist_score": optimist_score,
            "pessimist_score": pessimist_score,
            "risk_passed": risk_passed,
            "final_decision": vote["final_decision"],
            "buy_score": vote.get("buy_score", 0),
            "hold_score": vote.get("hold_score", 0),
            "sell_score": vote.get("sell_score", 0),
        }


_orchestrator_instance: Optional[DebateOrchestrator] = None


def get_orchestrator(api_key: Optional[str] = None) -> DebateOrchestrator:
    """获取编排器单例"""
    global _orchestrator_instance
    if _orchestrator_instance is None:
        api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
        _orchestrator_instance = DebateOrchestrator(api_key)
    return _orchestrator_instance
