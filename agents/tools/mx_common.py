# -*- coding: utf-8 -*-
"""
妙想 Skills 公共加载工具
"""
import asyncio
import importlib.util
import os
from pathlib import Path
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)


def ensure_mx_api_key() -> str:
    """
    确保妙想 API Key 已加载到环境变量。
    支持本地 .env 文件与部署环境密钥变量回退。
    """
    for env_name in ("EM_API_KEY", "MIAOXIANG_EM_API_KEY", "GITEA_SECRET_EM_API_KEY", "GITEA_EM_API_KEY"):
        value = str(os.environ.get(env_name, "") or "").strip()
        if value:
            os.environ["EM_API_KEY"] = value
            return value

    root = Path(__file__).resolve().parents[2]
    for env_name in (".env.local", ".env"):
        env_path = root / env_name
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() != "EM_API_KEY":
                    continue
                value = value.strip().strip('"').strip("'")
                if value:
                    os.environ["EM_API_KEY"] = value
                    return value
        except Exception as e:
            logger.warning(f"读取妙想环境配置失败 {env_path}: {e}")

    return ""


def load_mx_module(module_name: str, relative_script_path: str) -> Any:
    """
    按脚本路径动态加载妙想模块。
    """
    ensure_mx_api_key()
    root = Path(__file__).resolve().parents[2]
    module_path = root / relative_script_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载妙想脚本: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_async(coro: Any) -> Any:
    """
    在同步上下文中运行异步协程。
    """
    return asyncio.run(coro)
