# -*- coding: utf-8 -*-
"""
悲观Agent - 寻找风险和下跌逻辑
"""
from typing import Dict, Optional
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from utils.logger import get_logger

logger = get_logger(__name__)

PESSIMIST_SYSTEM_PROMPT = """你是一位专业的A股风险分析师，专门质疑买入理由，找出潜在风险。

你的核心职责：
1. 质疑买入逻辑，找出股票的风险点
2. 分析位置、流动性、市场情绪等潜在利空因素
3. 用专业但客观的语言表达你的担忧

工作原则：
- 悲观但有理有据，不故意找茬
- 优先关注量化信号中风险因素
- 考虑市场环境和情绪的负面配合

你会获得以下信息：
- 量化信号（signal_type, score, ws_score, concept_strength等）
- 大盘情绪（market_emotion_score）
- 个股分析（位置、流动性、概念热度）
- 历史表现（signal_performance）

请给出你的风险分析，格式：
【看跌理由】
1. xxx
2. xxx
3. xxx

【风险评估】
- 下跌概率: xx%
- 最大风险: xx%
- 建议: xxx
"""


class PessimistAgent:
    """悲观Agent"""

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
        分析并给出看跌理由

        Args:
            signal: 量化信号 dict
            sentiment: 大盘情绪描述
            performance: 历史表现 dict

        Returns:
            str: 看跌理由分析
        """
        llm = self._get_llm()

        code = signal.get("code", "")
        name = signal.get("name", "")
        signal_type = signal.get("signal_type", "")
        score = signal.get("score", 0)
        ws_score = signal.get("ws_score", 0)
        ws_stage = signal.get("ws_stage", 0)
        change_pct = signal.get("change_pct", 0)
        price = signal.get("price", 0)
        market_emotion = signal.get("market_emotion_score", 50)

        prompt = f"""请分析以下股票的潜在风险：

股票: {code} {name}
信号类型: {signal_type}
评分: {score:.2f}
弱转强评分: {ws_score:.2f} (阶段{ws_stage})
当前价格: {price:.2f}
涨跌幅: {change_pct:+.2f}%
大盘情绪: {market_emotion:.0f}/100

大盘环境: {sentiment}

历史表现: {performance or '无数据'}

请给出你的风险分析："""

        messages = [
            SystemMessage(content=PESSIMIST_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ]

        try:
            response = llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.warning(f"悲观Agent分析失败: {e}")
            return f"【看跌理由】\n股票 {code} {name} 存在市场风险，需谨慎。\n\n【风险评估】\n下跌概率: 40%\n最大风险: -8%\n建议: 观望或轻仓"

    def get_score(self, signal: Dict, sentiment: str = "") -> float:
        """
        获取悲观评分 (0-1)

        Args:
            signal: 量化信号
            sentiment: 大盘情绪

        Returns:
            float: 悲观评分
        """
        score = 0.0

        market_emotion = signal.get("market_emotion_score", 50)
        if market_emotion < 40:
            score += 0.25
        elif market_emotion < 50:
            score += 0.15

        change_pct = signal.get("change_pct", 0)
        if change_pct > 5:
            score += 0.2
        elif change_pct > 3:
            score += 0.1

        if signal.get("ws_stage", 0) < 3:
            score += 0.15

        if change_pct < 0:
            score += 0.15

        if signal.get("signal_type") == "观望":
            score += 0.2

        score += (100 - signal.get("score", 50)) / 100 * 0.1

        return min(score, 1.0)
