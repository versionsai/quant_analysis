# -*- coding: utf-8 -*-
"""
风控Agent - 规则检查和风险管理
"""
import os
from typing import Dict, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from config.config import STRATEGY_CONFIG, TRADING_CONFIG
from data.recommend_db import get_db
from utils.logger import get_logger

logger = get_logger(__name__)

RISK_AGENT_SYSTEM_PROMPT = """你是一位严格的A股风控专家，遵守交易纪律是你的天职。

你的核心职责：
1. 检查信号是否触发风控规则（止损线、仓位限制、情绪门控、时间止损）
2. 有一票否决权 - 如果触发硬规则，必须否决
3. 给出明确的风控结论

工作原则：
- 规则优先，不感情用事
- 软规则给建议，硬规则必须执行
- 记录所有风控检查结果

风控规则（来自config）：
- 止损线: {stop_loss}%
- 跟踪止盈: {trailing_stop}%
- 最大持仓: {max_stocks}只
- 单票最大仓位: {max_position}%
- 最长持仓: {max_hold_days}天
- 时间止损: {time_stop_days}天不涨
- 情绪门控: {emotion_enabled} (低于{market_emotion_stop_score}分触发)

请给出风控结论，格式：
【风控检查】
- 通过项: xxx
- 警告项: xxx
- 否决项: xxx（如有）

【最终结论】
- 通过/否决
- 原因: xxx
"""


class RiskAgent:
    """风控Agent"""

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-ai/DeepSeek-V3"):
        self.api_key = api_key
        self.model = model
        self.llm = None
        self.config = STRATEGY_CONFIG
        self.trading_config = TRADING_CONFIG

    def _get_llm(self):
        if self.llm is None:
            api_key = self.api_key or os.environ.get("SILICONFLOW_API_KEY", "")
            self.llm = ChatOpenAI(
                model=self.model,
                api_key=api_key,
                base_url="https://api.siliconflow.cn/v1",
                temperature=0.3,
            )
        return self.llm

    def check_rules(
        self,
        signal: Dict,
        position_count: int = 0,
    ) -> Dict:
        """
        检查风控规则（快速版本，不调用LLM）

        Args:
            signal: 量化信号
            position_count: 当前持仓数

        Returns:
            dict: {
                passed: bool,
                veto_reason: str,
                warnings: list,
                checks: dict
            }
        """
        warnings = []
        checks = {}
        veto_reason = None

        if signal.get("signal_type") != "买入":
            return {"passed": True, "warnings": [], "checks": {}, "veto_reason": None}

        cfg = self.config
        trad_cfg = self.trading_config

        max_stocks = trad_cfg.get("max_stocks", 10)
        if position_count >= max_stocks:
            veto_reason = f"已达最大持仓数 {max_stocks}"
            checks["max_positions"] = {"passed": False, "detail": veto_reason}
            return {"passed": False, "warnings": warnings, "checks": checks, "veto_reason": veto_reason}
        checks["max_positions"] = {"passed": True, "detail": f"持仓{position_count}/{max_stocks}"}

        if cfg.get("emotion_enabled", True):
            min_emotion = cfg.get("market_emotion_stop_score", 40.0)
            emotion_score = signal.get("market_emotion_score", 50)
            if emotion_score < min_emotion - 10:
                veto_reason = f"大盘情绪过低 ({emotion_score:.0f} < {min_emotion:.0f})"
                checks["market_emotion"] = {"passed": False, "detail": veto_reason}
            elif emotion_score < min_emotion:
                warnings.append(f"大盘情绪偏低 ({emotion_score:.0f})")
                checks["market_emotion"] = {"passed": True, "detail": "情绪偏低但放行"}
            else:
                checks["market_emotion"] = {"passed": True, "detail": "情绪正常"}

        passed = veto_reason is None
        return {"passed": passed, "warnings": warnings, "checks": checks, "veto_reason": veto_reason}

    def analyze(
        self,
        signal: Dict,
        position_count: int = 0,
    ) -> str:
        """
        详细风控分析（带LLM）

        Args:
            signal: 量化信号
            position_count: 当前持仓数

        Returns:
            str: 风控分析报告
        """
        quick_check = self.check_rules(signal, position_count)

        code = signal.get("code", "")
        name = signal.get("name", "")
        signal_type = signal.get("signal_type", "")
        emotion_score = signal.get("market_emotion_score", 50)
        stock_emotion = signal.get("stock_emotion_score", 0)
        concept_strength = signal.get("concept_strength_score", 0)

        cfg = self.config

        prompt = f"""请对以下信号进行风控分析：

股票: {code} {name}
信号: {signal_type}
大盘情绪: {emotion_score:.0f}/100
个股情绪: {stock_emotion:.0f}/100
概念强度: {concept_strength:.2f}
当前持仓: {position_count}/{cfg.get('max_stocks', 10)}

快速检查结果:
- 通过: {quick_check['passed']}
- 否决原因: {quick_check.get('veto_reason') or '无'}
- 警告: {quick_check.get('warnings') or '无'}

风控规则:
- 止损线: {cfg.get('stop_loss')*100:.0f}%
- 跟踪止盈: {cfg.get('trailing_stop')*100:.0f}%
- 最大持仓: {cfg.get('max_stocks', 10)}只
- 情绪门控: {'启用' if cfg.get('emotion_enabled') else '关闭'}

请给出详细风控分析："""

        try:
            llm = self._get_llm()
            messages = [
                SystemMessage(content=RISK_AGENT_SYSTEM_PROMPT.format(
                    stop_loss=cfg.get("stop_loss") * 100,
                    trailing_stop=cfg.get("trailing_stop") * 100,
                    max_stocks=cfg.get("max_stocks", 10),
                    max_position=cfg.get("max_position", 0.2) * 100,
                    max_hold_days=cfg.get("max_hold_days", 3),
                    time_stop_days=cfg.get("time_stop_days", 2),
                    emotion_enabled=cfg.get("emotion_enabled", True),
                    market_emotion_stop_score=cfg.get("market_emotion_stop_score", 40),
                )),
                HumanMessage(content=prompt),
            ]
            response = llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.warning(f"风控Agent分析失败: {e}")
            passed = quick_check["passed"]
            return f"【风控检查】\n{'通过' if passed else '否決'}\n\n【最终结论】\n{'通过' if passed else '否決'}\n原因: {quick_check.get('veto_reason', '风险检查完成')}"

    def get_decision(self, signal: Dict, position_count: int = 0) -> str:
        """
        获取风控决策

        Args:
            signal: 量化信号
            position_count: 当前持仓数

        Returns:
            str: "通过" 或 "否决"
        """
        check = self.check_rules(signal, position_count)
        return "通过" if check["passed"] else "否决"
