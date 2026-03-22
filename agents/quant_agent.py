# -*- coding: utf-8 -*-
"""
量化交易 Agent。
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Dict, List, Optional

from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI

from agents.skills import get_skills_manager, load_skills
from agents.tools import (
    analyze_portfolio,
    analyze_stock,
    check_quant_signals,
    get_cls_telegraph_news,
    get_global_finance_news,
    get_holding_announcements,
    get_market_sentiment,
    get_policy_news,
    mx_query_financial_data,
    mx_query_macro_data,
    mx_screen_securities,
    mx_search_financial_news,
    push_news_report,
    push_report,
    ta_analyze_stock,
    ta_analyze_us_market,
    ta_market_sentiment,
)
from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_MODEL = "deepseek-ai/DeepSeek-V3"
DEFAULT_AGENT_TIMEOUT_SEC = 90
DEFAULT_BUY_DECISION_TIMEOUT_SEC = 60
DEFAULT_REPORT_TIMEOUT_SEC = 120

DEFAULT_SYSTEM_PROMPT = """你是一个专业的A股量化交易顾问，名为“量化大师”。

你的职责：
1. 分析量化策略产生的交易信号
2. 结合市场情绪和新闻信息给出交易建议
3. 评估持仓风险并提供调整建议
4. 生成清晰易懂的交易报告

交易原则：
- 严格遵守止盈止损纪律
- 单只股票仓位不超过20%
- 最多同时持有5只股票
- 总仓位不超过90%

信号解读：
- 买入信号：MACD金叉 + 价格站上20日均线 + 成交量放大
- 卖出信号：MACD死叉或触及止盈/止损点位
- 观望信号：无明确信号时保持空仓

输出格式要求：
- 使用中文回答
- 关键数据用表格展示
- 建议清晰明了，便于执行"""


def _resolve_system_prompt(system_prompt: Optional[str] = None) -> str:
    """解析 Agent 系统提示词。"""
    if system_prompt:
        return system_prompt

    try:
        skills_manager = get_skills_manager()
        agent_prompt = skills_manager.get_agent_prompt()
        if agent_prompt:
            logger.info("使用 agents/skills/config/agent.yaml 作为 Agent 系统提示词")
            return agent_prompt
    except Exception as e:
        logger.warning(f"读取 Agent prompt 配置失败，回退默认提示词: {e}")

    return DEFAULT_SYSTEM_PROMPT


class QuantAgent:
    """量化交易 Agent。"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.7,
        system_prompt: Optional[str] = None,
    ):
        self.api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
        self.model = self._resolve_model(model)
        self.temperature = temperature
        self.system_prompt = _resolve_system_prompt(system_prompt)

        self.llm: Optional[ChatOpenAI] = None
        self.agent: Any = None
        self.tools = [
            get_market_sentiment,
            analyze_portfolio,
            check_quant_signals,
            push_report,
            get_cls_telegraph_news,
            get_global_finance_news,
            get_policy_news,
            get_holding_announcements,
            mx_search_financial_news,
            mx_query_financial_data,
            mx_query_macro_data,
            mx_screen_securities,
            push_news_report,
            analyze_stock,
            ta_analyze_stock,
            ta_market_sentiment,
            ta_analyze_us_market,
        ]

    @staticmethod
    def _resolve_model(model: Optional[str] = None) -> str:
        """解析模型名称。"""
        explicit_model = str(model or "").strip()
        if explicit_model:
            return explicit_model

        env_model = str(os.environ.get("SILICONFLOW_MODEL", "")).strip()
        if env_model:
            return env_model

        return DEFAULT_MODEL

    @staticmethod
    def _read_timeout(env_name: str, default_value: int) -> int:
        """读取超时配置。"""
        raw_value = str(os.environ.get(env_name, default_value)).strip()
        try:
            timeout_sec = int(raw_value)
            return timeout_sec if timeout_sec > 0 else default_value
        except Exception:
            logger.warning(f"超时配置无效 {env_name}={raw_value}，回退 {default_value}s")
            return default_value

    @staticmethod
    def _normalize_result(result: Any) -> Dict[str, Any]:
        """统一整理 Agent 返回结构。"""
        if isinstance(result, dict):
            normalized = dict(result)
            normalized.setdefault("messages", [])
            normalized.setdefault("ok", True)
            return normalized

        if isinstance(result, str):
            return {
                "messages": [{"role": "assistant", "content": result}],
                "content": result,
                "ok": True,
            }

        content = str(result)
        return {
            "messages": [{"role": "assistant", "content": content}],
            "content": content,
            "ok": True,
        }

    @staticmethod
    def extract_text(result: Any) -> str:
        """提取 Agent 执行结果中的文本内容。"""
        if isinstance(result, str):
            return result

        if isinstance(result, dict):
            content = result.get("content")
            if isinstance(content, str) and content.strip():
                return content

            for message in reversed(result.get("messages", [])):
                if isinstance(message, dict):
                    content = str(message.get("content", "")).strip()
                else:
                    content = str(getattr(message, "content", "")).strip()
                if content:
                    return content

        return str(result)

    def initialize(self):
        """初始化 Agent。"""
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

    def _invoke_agent(self, task: str) -> Dict[str, Any]:
        """执行底层 Agent 调用。"""
        raw_result = self.agent.invoke({
            "messages": [{"role": "user", "content": task}]
        })
        normalized = self._normalize_result(raw_result)
        normalized["content"] = self.extract_text(normalized)
        return normalized

    def _run_with_timeout(self, task: str, timeout_sec: int, operation_name: str) -> Dict[str, Any]:
        """在超时保护下执行 Agent。"""
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(self._invoke_agent, task)
        try:
            return future.result(timeout=timeout_sec)
        except FutureTimeoutError:
            future.cancel()
            logger.error(f"{operation_name} 超时，已在 {timeout_sec}s 后自动降级")
            return {
                "messages": [],
                "content": f"{operation_name} 超时",
                "ok": False,
                "error": "timeout",
            }
        except Exception as e:
            logger.error(f"{operation_name} 失败: {e}")
            return {
                "messages": [],
                "content": f"{operation_name} 失败: {e}",
                "ok": False,
                "error": str(e),
            }
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def run(
        self,
        task: str,
        timeout_sec: Optional[int] = None,
        operation_name: str = "Agent任务",
    ) -> Dict[str, Any]:
        """
        运行 Agent 执行任务。

        Args:
            task: 用户任务描述
            timeout_sec: 超时时间（秒）
            operation_name: 任务名称

        Returns:
            统一结构的 Agent 执行结果
        """
        if self.agent is None:
            self.initialize()

        try:
            logger.info(f"Agent 执行任务: {task}")
            resolved_timeout = timeout_sec or self._read_timeout(
                "AI_AGENT_TIMEOUT_SEC",
                DEFAULT_AGENT_TIMEOUT_SEC,
            )
            return self._run_with_timeout(task, resolved_timeout, operation_name)
        except Exception as e:
            logger.error(f"Agent 执行失败: {e}")
            return {
                "messages": [],
                "content": f"Agent 执行失败: {str(e)}",
                "ok": False,
                "error": str(e),
            }

    def run_daily_analysis(self) -> Dict[str, Any]:
        """执行每日分析任务。"""
        task = """请执行以下任务：

1. 获取当前市场情绪和财经新闻
2. 获取量化策略的最新交易信号
3. 分析当前持仓情况
4. 综合以上信息，给出今日交易建议
5. 如果有重要建议，推送报告到手机

请用清晰的中文格式输出分析结果。"""
        return self.run(
            task,
            timeout_sec=self._read_timeout("AI_REPORT_TIMEOUT_SEC", DEFAULT_REPORT_TIMEOUT_SEC),
            operation_name="每日分析",
        )

    def run_trade_check(self) -> Dict[str, Any]:
        """执行交易检查任务。"""
        task = """请执行以下任务：

1. 检查当前持仓状态
2. 分析持仓股票的盈亏情况
3. 检查是否有触发止盈/止损条件的持仓
4. 生成交易检查报告
5. 将报告推送到手机

请用清晰的中文格式输出检查结果。"""
        return self.run(
            task,
            timeout_sec=self._read_timeout("AI_REPORT_TIMEOUT_SEC", DEFAULT_REPORT_TIMEOUT_SEC),
            operation_name="交易检查",
        )

    def run_news_report(self) -> Dict[str, Any]:
        """执行综合新闻报告任务。"""
        task = """请执行以下任务，生成一条完整的综合报告：

1. 获取全球金融市场动态（美股、港股、期货等）
2. 获取A股市场政策相关资讯（重大政策、宏观新闻）
3. 获取财联社最新电报快讯，并提炼对A股盘面的影响
4. 获取持仓个股的最新公告和新闻
5. 获取量化策略的最新交易信号
6. 分析上述资讯对持仓个股的利好/利空影响

重要：生成一条包含所有内容的综合报告，并使用 push_report 工具推送一次即可。
请用清晰的中文格式输出。"""
        return self.run(
            task,
            timeout_sec=self._read_timeout("AI_REPORT_TIMEOUT_SEC", DEFAULT_REPORT_TIMEOUT_SEC),
            operation_name="综合新闻报告",
        )

    def run_buy_decision(
        self,
        signals: str,
        sentiment: str,
        holdings: str,
        us_analysis: str = "",
    ) -> Dict[str, Any]:
        """
        根据市场信息做出买入决策。
        """
        task = f"""作为A股量化交易助手，请基于以下信息做出今日买入决策：

【外围市场 - 美股隔夜分析】
{us_analysis or "(暂无美股数据)"}

【A股量化信号】
{signals}

【A股市场情绪】
{sentiment}

【当前持仓】
{holdings}

请分析以上信息，决定今日是否执行买入操作。

决策规则：
1. 优先选择量化信号明确为“买入”的标的
2. 关注美股走势对A股的指示意义
3. 已有持仓且浮盈时，可考虑加仓
4. 已有持仓且浮亏时，不建议加仓
5. 最多持有5只股票，避免过度分散
6. 市场情绪极差时，无强烈信号应跳过

请以JSON格式输出决策结果：
{{
  "action": "buy" 或 "skip",
  "reason": "决策理由",
  "buy_list": ["代码1"],
  "skip_list": ["代码2"],
  "add_list": ["代码3"]
}}

只输出JSON，不要有其他内容。"""

        result = self.run(
            task,
            timeout_sec=self._read_timeout("AI_BUY_DECISION_TIMEOUT_SEC", DEFAULT_BUY_DECISION_TIMEOUT_SEC),
            operation_name="买入决策",
        )

        if not result.get("ok", False):
            return {
                "action": "skip",
                "reason": result.get("content") or "买入决策失败",
                "buy_list": [],
                "skip_list": [],
                "add_list": [],
            }

        try:
            for message in reversed(result.get("messages", [])):
                if isinstance(message, dict):
                    content = str(message.get("content", "")).strip()
                else:
                    content = str(getattr(message, "content", "")).strip()
                if content.startswith("{"):
                    return json.loads(content)
        except Exception as e:
            logger.warning(f"买入决策 JSON 解析失败: {e}")

        return {"action": "skip", "reason": "解析失败", "buy_list": [], "skip_list": [], "add_list": []}


_agent_instance: Optional[QuantAgent] = None


def get_quant_agent() -> QuantAgent:
    """获取全局 Agent 实例。"""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = QuantAgent()
    return _agent_instance


def init_quant_agent(
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> QuantAgent:
    """初始化量化 Agent。"""
    global _agent_instance

    resolved_api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
    if not resolved_api_key:
        raise ValueError("SILICONFLOW_API_KEY is required")

    load_skills()

    _agent_instance = QuantAgent(api_key=resolved_api_key, model=model)
    _agent_instance.initialize()
    return _agent_instance
