# -*- coding: utf-8 -*-
"""
策略调优建议器
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from agents.quant_agent import get_quant_agent, init_quant_agent
from utils.logger import get_logger

logger = get_logger(__name__)


class StrategyTuningAdvisor:
    """使用 AI Agent 复盘回测并生成受控调优建议。"""

    def __init__(self):
        self._agent = None

    def _get_agent(self):
        """按需初始化 Agent。"""
        if self._agent is not None:
            return self._agent
        api_key = str(os.environ.get("SILICONFLOW_API_KEY", "") or "").strip()
        if not api_key:
            return None
        try:
            self._agent = get_quant_agent()
            if getattr(self._agent, "agent", None) is None:
                self._agent = init_quant_agent(api_key=api_key)
            return self._agent
        except Exception as e:
            logger.warning(f"策略调优 Agent 初始化失败，回退规则建议: {e}")
            return None

    @staticmethod
    def _build_payload(strategy_name: str, result: Any) -> Dict[str, Any]:
        """构建输入给 Agent 的回测摘要。"""
        return {
            "strategy_name": strategy_name,
            "summary": {
                "total_return": float(getattr(result, "total_return", 0.0) or 0.0),
                "annual_return": float(getattr(result, "annual_return", 0.0) or 0.0),
                "sharpe_ratio": float(getattr(result, "sharpe_ratio", 0.0) or 0.0),
                "max_drawdown": float(getattr(result, "max_drawdown", 0.0) or 0.0),
                "win_rate": float(getattr(result, "win_rate", 0.0) or 0.0),
                "trade_count": int(len(getattr(result, "trades", []) or [])),
            },
            "benchmarks": getattr(result, "benchmark_metrics", {}) or {},
            "phases": getattr(result, "phase_metrics", []) or [],
            "signal_summary": getattr(result, "signal_summary", {}) or {},
        }

    @staticmethod
    def _fallback_review(strategy_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """没有 AI 时的规则化建议。"""
        summary = dict(payload.get("summary", {}) or {})
        phases = list(payload.get("phases", []) or [])
        signal_summary = dict(payload.get("signal_summary", {}) or {})
        suggestions = []

        if float(summary.get("max_drawdown", 0.0) or 0.0) <= -0.15:
            suggestions.append({
                "type": "risk",
                "target": "stop_loss/trailing_stop",
                "priority": "high",
                "reason": "最大回撤偏大，建议先收紧退出参数或降低单票上限。",
            })

        if float(signal_summary.get("gate_pass_rate", 1.0) or 1.0) > 0.8:
            suggestions.append({
                "type": "gate",
                "target": "candidate_gate",
                "priority": "medium",
                "reason": "信号放行率较高，建议提高候选阈值或增加市场环境门控。",
            })

        for phase in phases:
            if str(phase.get("phase", "")) == "下跌段" and float(phase.get("excess_return", 0.0) or 0.0) < 0:
                suggestions.append({
                    "type": "regime",
                    "target": "bear_phase_filter",
                    "priority": "high",
                    "reason": "下跌段跑输基准，建议单独增强弱市过滤和减仓规则。",
                })
                break

        return {
            "strategy_name": strategy_name,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "rule_fallback",
            "summary": f"{strategy_name} 已生成规则化调优建议。",
            "suggestions": suggestions,
            "experiments": [
                {
                    "name": "tighten_risk_guard",
                    "goal": "降低回撤并观察超额收益变化",
                    "risk_overrides": {
                        "stop_loss": -0.04,
                        "trailing_stop": 0.04,
                        "max_hold_days": 2,
                    },
                },
                {
                    "name": "raise_candidate_gate",
                    "goal": "降低噪声信号，观察胜率和夏普改善情况",
                    "candidate_gate_threshold": 0.55,
                },
                {
                    "name": "tighten_risk_and_gate",
                    "goal": "同时收紧风控和信号放行，观察弱市超额是否改善",
                    "risk_overrides": {
                        "stop_loss": -0.04,
                        "trailing_stop": 0.04,
                    },
                    "candidate_gate_threshold": 0.60,
                },
            ],
        }

    def review_backtest(self, strategy_name: str, result: Any) -> Dict[str, Any]:
        """读取回测结果并输出调优建议。"""
        payload = self._build_payload(strategy_name, result)
        agent = self._get_agent()
        if agent is None:
            return self._fallback_review(strategy_name, payload)

        task = (
            "你是A股量化策略调优助手。"
            "请根据给定的回测摘要、基准对比、分阶段表现和候选信号统计，"
            "提出 3 到 5 条最值得验证的调优建议。"
            "注意：你不能直接建议“自动上线”，只能建议后续实验。"
            "请优先关注：弱市表现、超额收益稳定性、信号放行率、回撤控制。"
            "实验字段允许使用 overrides（策略参数）、risk_overrides（风控参数）、candidate_gate_threshold（候选门槛）。"
            "请只输出 JSON，不要输出其他内容。\n\n"
            "JSON 格式:\n"
            "{\n"
            '  "summary": "一段中文总结",\n'
            '  "suggestions": [{"type":"risk/gate/regime/parameter","target":"参数或模块名","priority":"high/medium/low","reason":"中文理由"}],\n'
            '  "experiments": [{"name":"实验名","goal":"实验目标","overrides":{"策略参数":"建议值"},"risk_overrides":{"风控参数":"建议值"},"candidate_gate_threshold":0.55}]\n'
            "}\n\n"
            f"回测摘要: {json.dumps(payload, ensure_ascii=False, default=str)}"
        )
        result_payload = agent.run(task=task, timeout_sec=90, operation_name="策略调优建议")
        if not result_payload.get("ok", False):
            return self._fallback_review(strategy_name, payload)

        text = agent.extract_text(result_payload)
        try:
            parsed = agent._extract_json_payload(text)
        except Exception:
            parsed = {}
        if not parsed:
            return self._fallback_review(strategy_name, payload)
        parsed.setdefault("strategy_name", strategy_name)
        parsed.setdefault("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        parsed.setdefault("source", "ai")
        return parsed


def save_tuning_review(review: Dict[str, Any], output_dir: Optional[str] = None) -> Dict[str, str]:
    """保存调优建议报告。"""
    base_dir = Path(output_dir or "./runtime/reports/tuning")
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = base_dir / f"tuning_review_{ts}.json"
    md_path = base_dir / f"tuning_review_{ts}.md"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(review, f, ensure_ascii=False, indent=2)

    lines = [
        f"# 策略调优建议 - {review.get('strategy_name', '')}",
        "",
        f"- 生成时间: {review.get('generated_at', '')}",
        f"- 来源: {review.get('source', '')}",
        "",
        "## 总结",
        "",
        str(review.get("summary", "") or ""),
        "",
        "## 建议",
        "",
    ]
    for index, item in enumerate(review.get("suggestions", []) or [], 1):
        lines.append(f"{index}. [{item.get('priority', '')}] {item.get('target', '')}: {item.get('reason', '')}")
    lines.extend(["", "## 实验建议", ""])
    for index, item in enumerate(review.get("experiments", []) or [], 1):
        lines.append(f"{index}. {item.get('name', '')}: {item.get('goal', '')}")
        overrides = item.get("overrides", {})
        if overrides:
            lines.append(f"   overrides={json.dumps(overrides, ensure_ascii=False)}")

    with md_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    return {
        "json_path": str(json_path),
        "markdown_path": str(md_path),
    }
