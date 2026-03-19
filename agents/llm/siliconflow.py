# -*- coding: utf-8 -*-
"""
SiliconFlow LLM 封装
兼容 OpenAI SDK，支持 DeepSeek/Qwen/GPT 等模型
"""
import os
from typing import Optional

from langchain_openai import ChatOpenAI
from utils.logger import get_logger

logger = get_logger(__name__)


class SiliconFlowLLM:
    """SiliconFlow LLM 封装类"""

    DEFAULT_MODEL = "deepseek-ai/DeepSeek-V3"
    BASE_URL = "https://api.siliconflow.cn/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ):
        self.api_key = api_key or os.environ.get("SILICONFLOW_API_KEY", "")
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self._client: Optional[ChatOpenAI] = None

    def _get_client(self) -> ChatOpenAI:
        """获取 LLM 客户端"""
        if self._client is None:
            if not self.api_key:
                raise ValueError("SILICONFLOW_API_KEY is not set")

            self._client = ChatOpenAI(
                model=self.model,
                api_key=self.api_key,
                base_url=self.BASE_URL,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
            logger.info(f"SiliconFlow LLM initialized: {self.model}")
        return self._client

    def chat(self, messages: list, **kwargs):
        """发送对话请求"""
        client = self._get_client()
        return client.invoke(messages, **kwargs)

    def get_model(self) -> ChatOpenAI:
        """获取原始 LangChain 模型对象 (用于 DeepAgents)"""
        return self._get_client()


_llm_instance: Optional[SiliconFlowLLM] = None


def get_llm(
    api_key: Optional[str] = None,
    model: str = SiliconFlowLLM.DEFAULT_MODEL,
    **kwargs
) -> SiliconFlowLLM:
    """获取全局 LLM 实例"""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = SiliconFlowLLM(api_key=api_key, model=model, **kwargs)
    return _llm_instance


def set_api_key(api_key: str):
    """设置 API Key"""
    global _llm_instance
    _llm_instance = None
    os.environ["SILICONFLOW_API_KEY"] = api_key


def init_llm(
    model: str = SiliconFlowLLM.DEFAULT_MODEL,
    temperature: float = 0.7,
) -> SiliconFlowLLM:
    """初始化 LLM"""
    global _llm_instance
    api_key = os.environ.get("SILICONFLOW_API_KEY", "")
    _llm_instance = SiliconFlowLLM(
        api_key=api_key,
        model=model,
        temperature=temperature,
    )
    return _llm_instance
