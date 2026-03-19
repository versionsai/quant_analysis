# -*- coding: utf-8 -*-
"""
量化交易 Agent
基于 DeepAgents 框架构建的智能量化交易助手
"""
import os
from typing import Optional, Any

from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI

from agents.llm import SiliconFlowLLM, get_llm, init_llm
from agents.skills import load_skills, get_skills_manager
from agents.tools import (
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
from utils.logger import get_logger

logger = get_logger(__name__)


DEFAULT_SYSTEM_PROMPT = """你是一个专业的A股量化交易顾问，名为"量化大师"。

你的职责：
1. 分析量化策略产生的交易信号
2. 结合市场情绪和新闻信息给出交易建议
3. 评估持仓风险并提供调整建议
4. 生成清晰易懂的交易报告

交易原则：
- 严格遵守止盈止损纪律
- 单只股票仓位不超过30%
- 最大同时持仓3只股票
- 总仓位不超过90%

信号解读：
- 买入信号：MACD金叉 + 价格站上20日均线 + 成交量放大
- 卖出信号：MACD死叉或触及止盈/止损点位
- 观望信号：无明确信号时保持空仓

输出格式要求：
- 使用中文回答
- 关键数据用表格展示
- 建议清晰明了，便于执行"""


class QuantAgent:
    """量化交易 Agent"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "deepseek-ai/DeepSeek-V3",
        temperature: float = 0.7,
        system_prompt: Optional[str] = None,
    ):
        self.api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
        self.model = model
        self.temperature = temperature
        self.system_prompt = system_prompt or DEFAULT_SYSTEM_PROMPT

        self.llm: Optional[ChatOpenAI] = None
        self.agent: Any = None
        self.tools = [
            get_market_sentiment,
            analyze_portfolio,
            check_quant_signals,
            push_report,
            get_global_finance_news,
            get_policy_news,
            get_holding_announcements,
            push_news_report,
            analyze_stock,
        ]

    def initialize(self):
        """初始化 Agent"""
        if not self.api_key:
            raise ValueError("SILICONFLOW_API_KEY is not set")

        logger.info(f"初始化 Quant Agent，模型: {self.model}")

        self.llm = ChatOpenAI(
            model=self.model,
            api_key=self.api_key,
            base_url="https://api.siliconflow.cn/v1",
            temperature=self.temperature,
        )

        self.agent = create_deep_agent(
            model=self.llm,
            tools=self.tools,
            system_prompt=self.system_prompt,
        )

        logger.info("Quant Agent 初始化完成")

    def run(self, task: str) -> str:
        """
        运行 Agent 执行任务

        Args:
            task: 用户任务描述

        Returns:
            str: Agent 执行结果
        """
        if self.agent is None:
            self.initialize()

        try:
            logger.info(f"Agent 执行任务: {task}")

            result = self.agent.invoke({
                "messages": [{"role": "user", "content": task}]
            })

            return str(result)

        except Exception as e:
            logger.error(f"Agent 执行失败: {e}")
            return f"Agent 执行失败: {str(e)}"

    def run_daily_analysis(self) -> str:
        """执行每日分析任务"""
        task = """请执行以下任务：

1. 获取当前市场情绪和财经新闻
2. 获取量化策略的最新交易信号
3. 分析当前持仓情况
4. 综合以上信息，给出今日交易建议
5. 如果有重要建议，推送报告到手机

请用清晰的中文格式输出分析结果。"""

        return self.run(task)

    def run_trade_check(self) -> str:
        """执行交易检查任务"""
        task = """请执行以下任务：

1. 检查当前持仓状态
2. 分析持仓股票的盈亏情况
3. 检查是否有触发止盈/止损条件的持仓
4. 生成交易检查报告
5. 将报告推送到手机

请用清晰的中文格式输出检查结果。"""

        return self.run(task)

    def run_news_report(self) -> str:
        """执行综合新闻报告任务（包含资讯和荐股）"""
        task = """请执行以下任务，生成一条完整的综合报告：

1. 获取全球金融市场动态（美股、港股、期货等）
2. 获取A股市场政策相关资讯（重大政策、宏观新闻）
3. 获取持仓个股的最新公告和新闻
4. 获取量化策略的最新交易信号
5. 分析上述资讯对持仓个股的利好/利空影响

**重要：生成一条包含所有内容的综合报告，使用 push_report 工具推送一次即可**

报告格式要求：
- 使用 emoji 分类展示
- 【全球市场】- 美股、港股、期货动态
- 【A股政策】- 重大政策、宏观新闻
- 【持仓个股】- 利好/利空分析
- 【今日荐股】- 量化信号推荐的买入标的
- 【操作建议】- 综合以上信息的交易建议

推送示例：
📊 综合报告 (03-19 09:00)
━━━━━━━━━━━━━━━━━━━━
【全球市场】
• 美股涨跌...
• 港股动态...

【A股政策】
• 政策利好...
• 市场热点...

【持仓个股】
✅ 利好：xxx公告
⚠️ 利空：xxx风险

【今日荐股】
✅ 600036 招商银行 @39.92
   目标41.92(+5%) 止损38.72(-3%)

【操作建议】
当前空仓，关注xxx...
━━━━━━━━━━━━━━━━━━━━

请生成完整报告并使用 push_report 工具一次性推送。"""

        return self.run(task)


_agent_instance: Optional[QuantAgent] = None


def get_quant_agent() -> QuantAgent:
    """获取全局 Agent 实例"""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = QuantAgent()
    return _agent_instance


def init_quant_agent(
    api_key: Optional[str] = None,
    model: str = "deepseek-ai/DeepSeek-V3",
) -> QuantAgent:
    """初始化量化 Agent"""
    global _agent_instance

    api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")

    if not api_key:
        raise ValueError("SILICONFLOW_API_KEY is required")

    load_skills()

    _agent_instance = QuantAgent(api_key=api_key, model=model)
    _agent_instance.initialize()

    return _agent_instance
