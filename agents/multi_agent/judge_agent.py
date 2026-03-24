# -*- coding: utf-8 -*-
"""
裁判Agent - 综合决策
"""
import os
from typing import Dict, List, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from utils.logger import get_logger

logger = get_logger(__name__)

JUDGE_SYSTEM_PROMPT = """你是一位专业的A股投资决策委员会主席，负责综合各方意见给出最终决策。

你的核心职责：
1. 综合乐观Agent、悲观Agent、风控Agent的意见
2. 权衡利弊，给出最终买入/观望/卖出决策
3. 决策必须明确、可执行

决策权重：
- 风控Agent有一票否决权（如果否决，则最终决策必须是卖出/观望）
- 乐观和悲观Agent的权重可以通过performance调整
- 最终决策要考虑市场环境和持仓情况

你会获得以下信息：
- 原始信号（signal_type, score等）
- 乐观Agent的分析和评分
- 悲观Agent的分析和评分
- 风控Agent的结论（通过/否决）
- 当前持仓情况
- 历史表现数据

请给出最终决策，格式：
【决策摘要】
- 乐观评分: xx
- 悲观评分: xx
- 风控结论: 通过/否决
- 最终决策: 买入/观望/卖出

【决策理由】
1. xxx
2. xxx

【操作建议】
- 买入仓位: xx%（如适用）
- 目标价: xxx
- 止损价: xxx
"""


class JudgeAgent:
    """裁判Agent"""

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-ai/DeepSeek-V3"):
        self.api_key = api_key
        self.model = model
        self.llm = None
        self.optimist_weight = 0.30
        self.pessimist_weight = 0.25

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

    def calculate_vote(
        self,
        optimist_score: float,
        pessimist_score: float,
        risk_passed: bool,
    ) -> Dict:
        """
        计算投票结果

        Args:
            optimist_score: 乐观Agent评分 (0-1)
            pessimist_score: 悲观Agent评分 (0-1)
            risk_passed: 风控是否通过

        Returns:
            dict: {
                buy_score: float,
                hold_score: float,
                sell_score: float,
                final_decision: str
            }
        """
        if not risk_passed:
            return {
                "buy_score": 0,
                "hold_score": 0.3,
                "sell_score": 0.7,
                "final_decision": "卖出",
            }

        buy_score = optimist_score * self.optimist_weight
        buy_score += (1 - pessimist_score) * self.pessimist_weight
        buy_score += 0.45

        hold_score = 0.3 + pessimist_score * 0.3

        sell_score = pessimist_score * self.pessimist_weight
        sell_score += (1 - optimist_score) * self.optimist_weight * 0.5
        sell_score += 0.15

        if buy_score > hold_score and buy_score > sell_score:
            decision = "买入"
        elif sell_score > hold_score:
            decision = "卖出"
        else:
            decision = "观望"

        return {
            "buy_score": buy_score,
            "hold_score": hold_score,
            "sell_score": sell_score,
            "final_decision": decision,
        }

    def analyze(
        self,
        signal: Dict,
        optimist_view: str,
        pessimist_view: str,
        risk_result: Dict,
        holdings: List[Dict] = None,
        performance: Dict = None,
    ) -> str:
        """
        综合分析并给出最终决策

        Args:
            signal: 原始信号
            optimist_view: 乐观Agent分析
            pessimist_view: 悲观Agent分析
            risk_result: 风控结果
            holdings: 当前持仓
            performance: 历史表现

        Returns:
            str: 最终决策报告
        """
        llm = self._get_llm()

        code = signal.get("code", "")
        name = signal.get("name", "")
        signal_type = signal.get("signal_type", "")
        price = signal.get("price", 0)
        target = signal.get("target_price", 0)
        stop_loss = signal.get("stop_loss", 0)
        risk_passed = risk_result.get("passed", True)
        risk_reason = risk_result.get("veto_reason", "")

        holdings_text = f"当前持仓{len(holdings or [])}只" if holdings else "无持仓"

        prompt = f"""请给出最终投资决策：

股票: {code} {name}
当前价格: {price:.2f}
目标价: {target:.2f}
止损价: {stop_loss:.2f}
原始信号: {signal_type}
{holdings_text}

【乐观Agent观点】
{optimist_view}

【悲观Agent观点】
{pessimist_view}

【风控结论】
{'通过' if risk_passed else '否决'}
{'原因: ' + risk_reason if risk_reason else ''}

【历史表现】
{performance or '无数据'}

请给出最终决策："""

        messages = [
            SystemMessage(content=JUDGE_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]

        try:
            response = llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.warning(f"裁判Agent分析失败: {e}")
            vote = self.calculate_vote(0.5, 0.4, risk_passed)
            return f"【决策摘要】\n最终决策: {vote['final_decision']}\n\n【决策理由】\n基于规则计算得出\n\n【操作建议】\n{'买入' if vote['final_decision'] == '买入' else '观望' if vote['final_decision'] == '观望' else '卖出'}"

    def get_decision(
        self,
        signal: Dict,
        optimist_score: float = 0.5,
        pessimist_score: float = 0.5,
        risk_passed: bool = True,
    ) -> str:
        """
        快速决策（不调用LLM）

        Args:
            signal: 原始信号
            optimist_score: 乐观Agent评分
            pessimist_score: 悲观Agent评分
            risk_passed: 风控是否通过

        Returns:
            str: "买入" / "观望" / "卖出"
        """
        vote = self.calculate_vote(optimist_score, pessimist_score, risk_passed)
        return vote["final_decision"]
