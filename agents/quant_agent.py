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
    get_market_news_digest,
    get_market_sentiment,
    get_policy_news,
    get_symbol_news_digest,
    push_news_report,
    push_report,
    search_market_context,
    search_policy_context,
    search_symbol_context,
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
DEFAULT_REGIME_TIMEOUT_SEC = 30

DEFAULT_SYSTEM_PROMPT = """你是一个专业的A股量化交易执行顾问，名为“量化大师”。

你的核心职责：
1. 理解量化策略输出的买入、观望、卖出候选信号
2. 结合市场模式、大盘情绪、板块强度、个股情绪、盘口和外围环境做执行判断
3. 对当前持仓给出继续持有、加仓、减仓、卖出的明确建议
4. 生成简洁、可执行、贴近交易的中文结论

你的工作原则：
- 量化信号是候选，不是最终指令
- 优先尊重量化信号方向，但要结合当前市场环境做二次判断
- 弱市、退潮、冰点环境下要显著收紧买入和加仓
- 对已有持仓，要根据量化信号强弱、盈亏状态、市场环境决定持有、减仓或卖出
- 规则止损、硬风控优先级高于主观判断

你会接收到的量化信息通常包括：
- 信号类型（买入/观望/卖出）
- 评分、涨跌幅、FCF、双信号状态
- 大盘情绪、个股情绪、空间板、市场模式
- 概念强度、盘口买卖盘强弱
- ETF/LOF 分类（宽基、主题、防御、海外映射等）

输出要求：
- 使用中文
- 建议必须明确，可直接执行
- 不要脱离给定的量化信息空泛发挥
- 如果要求 JSON，只输出 JSON"""


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
            get_market_news_digest,
            get_symbol_news_digest,
            search_market_context,
            search_policy_context,
            search_symbol_context,
            get_policy_news,
            get_holding_announcements,
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
        4. 使用 get_market_news_digest 补充市场级资讯，重点查看政策、监管、行业热点与海外扰动
        5. 使用 get_symbol_news_digest 和 get_holding_announcements 补充持仓、信号池相关公告与事件
        6. 获取量化策略的最新交易信号
        7. 分析上述资讯对持仓个股和信号池标的的利好/利空影响

        重要：生成一条包含所有内容的综合报告，并使用 push_report 工具推送一次即可。
        输出时请明确分成“全球市场、政策宏观、市场资讯补充、持仓与信号池影响、执行建议”几部分。
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
        task = f"""作为A股量化交易执行助手，请基于以下信息做出今日买入决策：

【外围市场 - 美股隔夜分析】
{us_analysis or "(暂无美股数据)"}

【A股量化信号】
{signals}

【A股市场情绪】
{sentiment}

【当前持仓】
{holdings}

请分析以上信息，决定今日是否执行买入/加仓操作。

决策规则：
1. 量化信号列表中会同时出现买入、观望、卖出，请综合比较后做最终执行选择
2. 优先选择量化信号明确为“买入”、且环境共振更强的标的
3. 已有持仓且浮盈时，只有在量化信号继续强化、市场允许时才可加仓
4. 已有持仓且浮亏时，原则上不建议加仓
5. 最多持有5只股票，避免过度分散
6. 市场情绪极差、指数走弱、空间退潮时，无强烈共振应跳过
7. 观望信号和卖出信号也要作为风险参考，不要只盯买入候选

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

    def run_position_decision(
        self,
        holdings_signals: str,
        sentiment: str,
    ) -> Dict[str, Any]:
        """
        根据持仓量化信号做执行决策。
        """
        task = f"""作为A股量化交易执行助手，请基于以下信息对当前持仓做执行决策：

【持仓量化信号】
{holdings_signals}

【A股市场情绪】
{sentiment}

请注意：
1. 量化信号是候选依据，AI负责最终执行判断
2. 如果量化信号偏空、情绪退潮、个股走弱、盘口恶化，可考虑减仓或卖出
3. 如果量化信号继续偏多、趋势延续、且已有浮盈，可考虑加仓
4. 如果只是普通波动或量化信号不够一致，不要轻易交易
5. 你的目标是减少无效交易，同时尊重量化信号方向
6. 对减仓和卖出要给出明确代码和一句话原因

请以JSON格式输出：
{{
  "action": "manage" 或 "hold",
  "reason": "整体决策理由",
  "sell_list": ["代码1"],
  "reduce_list": ["代码2"],
  "add_list": ["代码3"],
  "hold_list": ["代码4"],
  "reasons": {{
    "代码1": "一句话原因",
    "代码2": "一句话原因"
  }}
}}

只输出JSON，不要有其他内容。"""

        result = self.run(
            task,
            timeout_sec=self._read_timeout("AI_BUY_DECISION_TIMEOUT_SEC", DEFAULT_BUY_DECISION_TIMEOUT_SEC),
            operation_name="持仓执行决策",
        )

        if not result.get("ok", False):
            return {
                "action": "hold",
                "reason": result.get("content") or "持仓执行决策失败",
                "sell_list": [],
                "reduce_list": [],
                "add_list": [],
                "hold_list": [],
                "reasons": {},
            }

        payload = self._extract_json_payload(self.extract_text(result))
        if not payload:
            return {
                "action": "hold",
                "reason": "解析失败",
                "sell_list": [],
                "reduce_list": [],
                "add_list": [],
                "hold_list": [],
                "reasons": {},
            }

        return {
            "action": str(payload.get("action", "hold") or "hold"),
            "reason": str(payload.get("reason", "") or "").strip(),
            "sell_list": list(payload.get("sell_list", []) or []),
            "reduce_list": list(payload.get("reduce_list", []) or []),
            "add_list": list(payload.get("add_list", []) or []),
            "hold_list": list(payload.get("hold_list", []) or []),
            "reasons": dict(payload.get("reasons", {}) or {}),
        }

    @staticmethod
    def _extract_json_payload(text: str) -> Dict[str, Any]:
        """从文本中提取 JSON 对象。"""
        content = str(text or "").strip()
        if not content:
            return {}
        candidates = [content]
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            candidates.insert(0, content[start:end + 1])

        for item in candidates:
            try:
                return json.loads(item)
            except Exception:
                continue
        return {}

    def judge_market_regime(
        self,
        market_snapshot: Dict[str, Any],
        candidate_mode: str = "auto",
    ) -> Dict[str, Any]:
        """
        使用 AI 判断当前市场更接近哪种运行模式。
        """
        task = (
            "你是A股盘中风格判断助手。请根据给定的指数、情绪、空间板、运行模式候选，"
            "判断当前更适合以下哪种模式之一：normal、defense、golden_pit。"
            "其中 normal=正常环境，defense=指数大跌或退潮时只做抱团防守，"
            "golden_pit=系统性杀跌后开始出现恐慌修复和黄金坑机会。"
            "请优先考虑指数表现、市场情绪、空间高度、是否存在资金抱团、是否只是普通反抽。"
            "如果信息不足，也必须在三者中选一个最稳妥的模式。"
            "请只输出 JSON，不要输出其他内容。\n\n"
            "JSON 格式:\n"
            "{\n"
            '  "mode": "normal 或 defense 或 golden_pit",\n'
            '  "reason": "一句到两句中文理由",\n'
            '  "confidence": 0到1之间的小数\n'
            "}\n\n"
            f"当前规则候选模式: {candidate_mode}\n"
            f"市场快照: {json.dumps(market_snapshot, ensure_ascii=False, default=str)}"
        )

        result = self.run(
            task,
            timeout_sec=self._read_timeout("AI_REGIME_TIMEOUT_SEC", DEFAULT_REGIME_TIMEOUT_SEC),
            operation_name="市场模式判断",
        )
        if not result.get("ok", False):
            return {
                "mode": candidate_mode if candidate_mode in {"normal", "defense", "golden_pit"} else "normal",
                "reason": result.get("content") or "市场模式判断失败",
                "confidence": 0.0,
                "source": "fallback",
            }

        payload = self._extract_json_payload(self.extract_text(result))
        mode = str(payload.get("mode", "") or "").strip().lower()
        if mode not in {"normal", "defense", "golden_pit"}:
            mode = candidate_mode if candidate_mode in {"normal", "defense", "golden_pit"} else "normal"
        confidence = payload.get("confidence", 0.0)
        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.0
        return {
            "mode": mode,
            "reason": str(payload.get("reason", "") or self.extract_text(result)).strip(),
            "confidence": max(0.0, min(confidence, 1.0)),
            "source": "ai",
        }

    def review_signal_with_regime(
        self,
        regime_mode: str,
        signal_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        使用 AI 对候选信号做模式内二次放行。
        """
        task = (
            "你是A股量化信号二次审核助手。"
            f"当前市场模式为 {regime_mode}。"
            "请结合给定的股票信号、量价、情绪、概念和盘口信息，判断这个标的现在更适合："
            "buy（允许买入）、watch（继续观望）、skip（直接跳过）。"
            "如果是 defense 模式，要优先考虑是否属于抱团核心；"
            "如果是 golden_pit 模式，要优先判断是否真的是恐慌后的放量修复，而不是普通反抽。"
            "请只输出 JSON，不要输出其他内容。\n\n"
            "JSON 格式:\n"
            "{\n"
            '  "decision": "buy 或 watch 或 skip",\n'
            '  "reason": "一句到两句中文理由",\n'
            '  "confidence": 0到1之间的小数\n'
            "}\n\n"
            f"候选信号: {json.dumps(signal_payload, ensure_ascii=False, default=str)}"
        )

        result = self.run(
            task,
            timeout_sec=self._read_timeout("AI_REGIME_TIMEOUT_SEC", DEFAULT_REGIME_TIMEOUT_SEC),
            operation_name="模式内信号审核",
        )
        if not result.get("ok", False):
            return {
                "decision": "watch",
                "reason": result.get("content") or "模式内信号审核失败",
                "confidence": 0.0,
                "source": "fallback",
            }

        payload = self._extract_json_payload(self.extract_text(result))
        decision = str(payload.get("decision", "") or "").strip().lower()
        if decision not in {"buy", "watch", "skip"}:
            decision = "watch"
        confidence = payload.get("confidence", 0.0)
        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.0
        return {
            "decision": decision,
            "reason": str(payload.get("reason", "") or self.extract_text(result)).strip(),
            "confidence": max(0.0, min(confidence, 1.0)),
            "source": "ai",
        }


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
