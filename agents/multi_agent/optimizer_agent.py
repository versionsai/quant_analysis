# -*- coding: utf-8 -*-
"""
优化Agent - 每日自优化复盘
"""
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from agents.multi_agent.wfa_engine import WFAEngine
from data.recommend_db import get_db, SignalQualityDB, DynamicParamsDB, ManualOverrideDB
from utils.logger import get_logger

logger = get_logger(__name__)

OPTIMIZER_SYSTEM_PROMPT = """你是一位专业的A股策略优化专家，负责每日复盘并调整参数。

你的核心职责：
1. 分析当日交易结果（胜率、盈亏）
2. 评估各信号来源和Agent的表现
3. 基于WFA（Walk-Forward）分析调整参数
4. 防止过拟合 - 只做小幅调整

优化目标：
- 胜率 >= 55%
- 最大回撤 <= 8%
- 总收益 >= 20%

你会获得：
- 当日交易摘要
- 信号来源表现
- Agent表现
- WFA稳定性分数
- 当前参数

请给出优化建议，格式：
【当日复盘】
- 交易次数: xx
- 胜率: xx%
- 总盈亏: xx

【原因分析】
1. xxx

【参数调整建议】
- {param_name}: {old_value} -> {new_value}
- 原因: xxx

【稳定性评估】
- WFA分数: xx
- 是否可调整: 是/否
"""


class OptimizerAgent:
    """优化Agent - 每日自优化"""

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-ai/DeepSeek-V3"):
        self.api_key = api_key
        self.model = model
        self.llm = None
        self.wfa_engine = WFAEngine()
        self.sq_db = SignalQualityDB()
        self.dp_db = DynamicParamsDB()
        self.override_db = ManualOverrideDB()

    def _get_llm(self):
        if self.llm is None:
            api_key = self.api_key or os.environ.get("SILICONFLOW_API_KEY", "")
            self.llm = ChatOpenAI(
                model=self.model,
                api_key=api_key,
                base_url="https://api.siliconflow.cn/v1",
                temperature=0.5,
            )
        return self.llm

    def get_daily_summary(self) -> Dict:
        """获取当日交易摘要"""
        db = get_db()
        today = datetime.now().strftime("%Y-%m-%d")
        
        trades = db.get_trade_history(days=1)
        today_trades = [t for t in trades if str(t.get("date", "")) == today]
        
        if not today_trades:
            return {
                "date": today,
                "total_trades": 0,
                "win_trades": 0,
                "win_rate": 0,
                "total_pnl": 0,
            }
        
        win_trades = sum(1 for t in today_trades if float(t.get("pnl", 0) or 0) > 0)
        total_pnl = sum(float(t.get("pnl", 0) or 0) for t in today_trades)
        
        return {
            "date": today,
            "total_trades": len(today_trades),
            "win_trades": win_trades,
            "win_rate": win_trades / len(today_trades) * 100,
            "total_pnl": total_pnl,
        }

    def get_performance_analysis(self, lookback_days: int = 30) -> Dict:
        """获取表现分析"""
        source_perf = self.sq_db.get_performance_by_source(lookback_days)
        agent_perf = self.sq_db.get_performance_by_agent(lookback_days)
        
        return {
            "by_source": source_perf,
            "by_agent": agent_perf,
        }

    def run_daily_optimization(self) -> Dict:
        """
        执行每日优化（带辩论机制）
        
        Returns:
            dict: 优化结果
        """
        logger.info("开始每日优化...")
        
        daily_summary = self.get_daily_summary()
        performance = self.get_performance_analysis()
        current_params = self.dp_db.get_all_params()
        stability_score = self.wfa_engine.wfa_db.get_latest_stability_score()
        
        suggestions = self._generate_param_suggestions(
            daily_summary,
            performance,
            current_params,
            stability_score,
        )
        
        applied_changes = []
        rejected_changes = []
        
        for change in suggestions:
            param_key = change.get("param_key")
            new_value = change.get("new_value")
            reason = change.get("reason", "每日优化")
            
            if not self.wfa_engine.is_param_change_safe(param_key, new_value):
                rejected_changes.append({
                    **change,
                    "rejected_reason": "WFA稳定性检查未通过"
                })
                continue
            
            debate_result = self._run_debate_for_param(param_key, new_value, reason, stability_score)
            
            if debate_result.get("approved", False):
                self.dp_db.set_param(
                    key=param_key,
                    value=new_value,
                    reason=reason + f" (辩论通过: {debate_result.get('summary', '')})",
                    source="optimizer_debate",
                )
                applied_changes.append({**change, "debate_result": debate_result})
                logger.info(f"参数 {param_key} 调整已通过辩论: {new_value}")
            else:
                rejected_changes.append({
                    **change,
                    "rejected_reason": debate_result.get("summary", "辩论未通过"),
                    "debate_result": debate_result
                })
                logger.info(f"参数 {param_key} 调整被辩论拒绝: {debate_result.get('summary', '')}")
        
        result = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "daily_summary": daily_summary,
            "performance": performance,
            "suggestions": suggestions,
            "applied_changes": applied_changes,
            "rejected_changes": rejected_changes,
            "stability_score": stability_score,
        }
        
        self._save_optimization_to_db(result)
        
        return result

    def _save_optimization_to_db(self, result: Dict):
        """保存优化结果到数据库"""
        try:
            from data.recommend_db import DailyOptimizationDB
            opt_db = DailyOptimizationDB()
            opt_db.add_optimization(
                date=result.get("date", ""),
                daily_summary=result.get("daily_summary", {}),
                performance=result.get("performance", {}),
                suggestions=result.get("suggestions", []),
                applied_changes=result.get("applied_changes", []),
                rejected_changes=result.get("rejected_changes", []),
                stability_score=result.get("stability_score"),
            )
            logger.info("优化结果已保存到数据库")
        except Exception as e:
            logger.warning(f"保存优化结果失败: {e}")

    def _run_debate_for_param(self, param_key: str, new_value: float, reason: str, stability_score: Optional[float]) -> Dict:
        """运行辩论来评估参数建议"""
        from agents.multi_agent.debate_orchestrator import get_orchestrator
        
        try:
            orchestrator = get_orchestrator()
            
            signal = {
                "code": "PARAM",
                "name": f"参数调整: {param_key}",
                "signal_type": "买入",
                "score": 70,
                "ws_stage": 3,
                "ws_score": 30,
                "price": new_value,
                "change_pct": 0,
                "market_emotion_score": 50,
                "dual_signal": True,
                "concept_strength_score": 0.7,
            }
            
            result = orchestrator.run_quick(signal, position_count=5)
            
            decision = result.get("final_decision", "观望")
            buy_score = result.get("buy_score", 0)
            
            if decision == "买入" and buy_score > 0.6:
                return {
                    "approved": True,
                    "summary": f"乐观评分{result.get('optimist_score', 0):.2f}, 悲观评分{result.get('pessimist_score', 0):.2f}",
                    "optimist_score": result.get("optimist_score", 0),
                    "pessimist_score": result.get("pessimist_score", 0),
                    "risk_passed": result.get("risk_passed", True),
                }
            else:
                return {
                    "approved": False,
                    "summary": f"决策:{decision}, 买入分数:{buy_score:.2f}",
                    "optimist_score": result.get("optimist_score", 0),
                    "pessimist_score": result.get("pessimist_score", 0),
                    "risk_passed": result.get("risk_passed", True),
                }
                
        except Exception as e:
            logger.warning(f"参数辩论失败: {e}")
            return {"approved": False, "summary": f"辩论执行异常: {e}"}

    def _generate_param_suggestions(
        self,
        daily_summary: Dict,
        performance: Dict,
        current_params: Dict,
        stability_score: Optional[float],
    ) -> List[Dict]:
        """生成参数调整建议"""
        suggestions = []
        
        source_perf = performance.get("by_source", {})
        
        if source_perf:
            best_source = max(source_perf.keys(), key=lambda k: source_perf[k].get("win_rate", 0))
            worst_source = min(source_perf.keys(), key=lambda k: source_perf[k].get("win_rate", 0))
            
            best_win_rate = source_perf[best_source].get("win_rate", 0)
            worst_win_rate = source_perf[worst_source].get("win_rate", 0)
            
            if best_win_rate > 60 and worst_win_rate < 45:
                suggestions.append({
                    "param_key": "gate_threshold",
                    "old_value": current_params.get("gate_threshold", {}).get("value"),
                    "new_value": current_params.get("gate_threshold", {}).get("value", 0.58) + 0.02,
                    "reason": f"信号来源{best_source}胜率较高({best_win_rate:.0f}%)，适当收紧门控",
                })
        
        total_trades = sum(p.get("total", 0) for p in source_perf.values())
        if total_trades > 50:
            avg_win_rate = sum(p.get("win_rate", 0) * p.get("total", 0) for p in source_perf.values()) / total_trades
            if avg_win_rate < 50:
                suggestions.append({
                    "param_key": "gate_threshold",
                    "old_value": current_params.get("gate_threshold", {}).get("value"),
                    "new_value": current_params.get("gate_threshold", {}).get("value", 0.58) + 0.03,
                    "reason": f"整体胜率偏低({avg_win_rate:.0f}%)，收紧门控以减少低质量信号",
                })
        
        if stability_score is not None and stability_score < 0.6:
            suggestions.append({
                "param_key": "max_position",
                "old_value": current_params.get("max_position", {}).get("value"),
                "new_value": max(0.15, current_params.get("max_position", {}).get("value", 0.18) - 0.02),
                "reason": f"WFA稳定性较低({stability_score:.2f})，降低仓位控制风险",
            })
        
        return suggestions

    def analyze_and_suggest(self) -> str:
        """AI驱动的分析和建议"""
        llm = self._get_llm()
        
        daily_summary = self.get_daily_summary()
        performance = self.get_performance_analysis()
        current_params = self.dp_db.get_all_params()
        stability_score = self.wfa_engine.wfa_db.get_latest_stability_score()
        
        prompt = f"""请分析以下数据并给出优化建议：

【当日交易】
{daily_summary}

【历史表现（30天）】
{json.dumps(performance, ensure_ascii=False, indent=2)}

【当前参数】
{json.dumps({k: v.get('value') for k, v in current_params.items()}, ensure_ascii=False)}

【WFA稳定性】
{json.dumps({'score': stability_score}, ensure_ascii=False)}

请给出优化建议："""

        messages = [
            SystemMessage(content=OPTIMIZER_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]

        try:
            response = llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.warning(f"优化Agent分析失败: {e}")
            return "优化分析失败，使用规则引擎建议"

    def record_outcome(self, signal_id: str, exit_date: str, exit_price: float) -> bool:
        """记录出场结果"""
        db = get_db()
        holdings = db.get_holdings_aggregated()
        
        holding = None
        for h in holdings:
            if str(h.get("code", "")) in signal_id:
                holding = h
                break
        
        if not holding:
            logger.warning(f"未找到持仓 {signal_id}")
            return False
        
        entry_price = float(holding.get("avg_buy_price", 0))
        pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
        outcome = "win" if pnl_pct > 0 else "loss"
        
        from datetime import datetime
        holding_days = (datetime.now() - datetime.strptime(holding.get("first_buy_date", ""), "%Y-%m-%d")).days
        
        return self.sq_db.update_signal_outcome(
            signal_id=signal_id,
            exit_date=exit_date,
            exit_price=exit_price,
            holding_days=holding_days,
            pnl_pct=pnl_pct * 100,
            outcome=outcome,
        )


_optimizer_instance: Optional[OptimizerAgent] = None


def get_optimizer(api_key: Optional[str] = None) -> OptimizerAgent:
    """获取优化Agent单例"""
    global _optimizer_instance
    if _optimizer_instance is None:
        api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
        _optimizer_instance = OptimizerAgent(api_key)
    return _optimizer_instance
