# -*- coding: utf-8 -*-
"""
乐观Agent - 寻找买入机会和上涨逻辑
"""
from typing import Dict, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from utils.logger import get_logger

logger = get_logger(__name__)

OPTIMIST_SYSTEM_PROMPT = """你是一位专业的A股量化分析师，擅长发现股票的上涨逻辑和投资机会。

你的核心职责：
1. 分析股票的潜在利好因素（概念热度、业绩拐点、资金流入、突破形态）
2. 结合量化信号和市场环境，找出支持买入的逻辑
3. 用专业但易懂的语言表达你的分析

工作原则：
- 乐观但有理有据，不盲目推荐
- 优先关注量化信号中积极的因素
- 考虑市场环境和情绪的配合

你会获得以下信息：
- 量化信号（signal_type, score, ws_score, concept_strength等）
- 大盘情绪（market_emotion_score）
- 个股分析（概念、资金流向、技术形态）
- 历史表现（signal_performance）

请给出你的买入理由分析，格式：
【看涨理由】
1. xxx
2. xxx
3. xxx

【综合评估】
- 上涨概率: xx%
- 目标涨幅: xx%
- 风险提示: xxx
"""


class OptimistAgent:
    """乐观Agent"""

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-ai/DeepSeek-V3"):
        self.api_key = api_key
        self.model = model
        self.llm = None

    def _get_llm(self):
        if self.llm is None:
            import os
            api_key = self.api_key or os.environ.get("SILICONFLOW_API_KEY", "")
            self.llm = ChatOpenAI(
                model=self.model,
                api_key=api_key,
                base_url="https://api.siliconflow.cn/v1",
                temperature=0.7,
            )
        return self.llm

    def analyze(
        self,
        signal: Dict,
        sentiment: str = "",
        performance: Dict = None,
    ) -> str:
        """
        分析并给出看涨理由

        Args:
            signal: 量化信号 dict
            sentiment: 大盘情绪描述
            performance: 历史表现 dict

        Returns:
            str: 看涨理由分析
        """
        llm = self._get_llm()

        code = signal.get("code", "")
        name = signal.get("name", "")
        signal_type = signal.get("signal_type", "")
        score = signal.get("score", 0)
        ws_score = signal.get("ws_score", 0)
        ws_stage = signal.get("ws_stage", 0)
        concept_name = signal.get("concept_name", "")
        concept_strength = signal.get("concept_strength_score", 0)
        dual_signal = signal.get("dual_signal", False)

        prompt = f"""请分析以下股票的买入机会：

股票: {code} {name}
信号类型: {signal_type}
评分: {score:.2f}
弱转强评分: {ws_score:.2f} (阶段{ws_stage})
概念: {concept_name} (强度{concept_strength:.2f})
双重信号: {'是' if dual_signal else '否'}
当前价格: {signal.get('price', 0):.2f}
涨跌幅: {signal.get('change_pct', 0):+.2f}%

大盘环境: {sentiment}

历史表现: {performance or '无数据'}

请给出你的看涨理由分析："""

        messages = [
            SystemMessage(content=OPTIMIST_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]

        try:
            response = llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.warning(f"乐观Agent分析失败: {e}")
            return f"【看涨理由】\n股票 {code} {name} 量化信号积极，建议关注。\n\n【综合评估】\n上涨概率: 60%\n目标涨幅: 5-10%\n风险提示: 市场风险"

    def get_score(self, signal: Dict, sentiment: str = "") -> float:
        """
        获取乐观评分 (0-1)

        Args:
            signal: 量化信号
            sentiment: 大盘情绪

        Returns:
            float: 乐观评分
        """
        score = 0.0

        if signal.get("signal_type") == "买入":
            score += 0.3

        if signal.get("ws_stage", 0) >= 3:
            score += 0.2

        if signal.get("dual_signal"):
            score += 0.15

        if signal.get("concept_strength_score", 0) > 0.6:
            score += 0.15

        if signal.get("change_pct", 0) > 0:
            score += 0.1

        score += signal.get("score", 0) / 100 * 0.1

        return min(score, 1.0)
