# -*- coding: utf-8 -*-
"""
Skills 管理器
动态加载 YAML 配置，支持本地+远程混合模式
"""
import os
import time
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)


class SkillsManager:
    """Skills 动态配置管理器"""

    def __init__(
        self,
        local_path: str = "./agents/skills/config",
        refresh_interval: int = 3600,
    ):
        self.local_path = Path(local_path)
        self.refresh_interval = refresh_interval
        self.last_refresh = 0
        self.skills: Dict[str, Any] = {}

        self.remote_url = os.environ.get("GITEA_SKILLS_URL", "")
        self.remote_token = os.environ.get("GITEA_SKILLS_TOKEN", "")
        self.remote_repo = os.environ.get("GITEA_SKILLS_REPO", "quant-agent-skills")

    def load_all(self):
        """启动时加载所有 Skills"""
        logger.info("加载 Skills 配置...")
        self._load_local_skills()
        self._fetch_remote_skills()
        self.last_refresh = time.time()
        logger.info(f"Skills 加载完成: {list(self.skills.keys())}")

    def refresh_if_needed(self):
        """定时刷新远程配置"""
        if not self.remote_url:
            return

        if time.time() - self.last_refresh > self.refresh_interval:
            logger.info("刷新远程 Skills 配置...")
            self._fetch_remote_skills()
            self.last_refresh = time.time()

    def _load_local_skills(self):
        """加载本地 YAML 配置"""
        if not self.local_path.exists():
            logger.warning(f"本地 Skills 目录不存在: {self.local_path}")
            return

        for yaml_file in self.local_path.glob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                    name = yaml_file.stem
                    self.skills[name] = config
                    logger.info(f"加载本地 Skill: {name}")
            except Exception as e:
                logger.error(f"加载 {yaml_file} 失败: {e}")

    def _fetch_remote_skills(self):
        """从 Gitea 获取远程配置"""
        if not self.remote_url or not self.remote_token:
            logger.info("未配置远程 Skills，跳过")
            return

        try:
            import requests

            headers = {"Authorization": f"token {self.remote_token}"}
            base_url = f"{self.remote_url}/repos/{self.remote_repo}/contents/agents/skills/config"

            response = requests.get(base_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"获取远程 Skills 失败: {response.status_code}")
                return

            files = response.json()
            if not isinstance(files, list):
                return

            for file_info in files:
                if file_info.get("name", "").endswith(".yaml"):
                    content_url = file_info.get("url", "")
                    if content_url:
                        content_response = requests.get(content_url, headers=headers, timeout=10)
                        if content_response.status_code == 200:
                            import base64

                            content = base64.b64decode(content_response.json()["content"])
                            config = yaml.safe_load(content)
                            name = file_info["name"].replace(".yaml", "")
                            self.skills[name] = config
                            logger.info(f"加载远程 Skill: {name}")

        except Exception as e:
            logger.error(f"获取远程 Skills 失败: {e}")

    def get_skill(self, name: str) -> Dict[str, Any]:
        """获取指定 Skill 配置"""
        return self.skills.get(name, {})

    def get_signal_params(self) -> Dict[str, Any]:
        """获取信号策略参数"""
        skill = self.get_skill("signal")
        return skill.get("params", {})

    def get_risk_rules(self) -> Dict[str, Any]:
        """获取风控规则"""
        skill = self.get_skill("risk")
        return skill.get("rules", {})

    def get_agent_prompt(self) -> str:
        """获取 Agent 系统提示词"""
        skill = self.get_skill("agent")
        return skill.get("prompt", "")


_skills_manager: Optional[SkillsManager] = None


def get_skills_manager() -> SkillsManager:
    """获取全局 Skills 管理器"""
    global _skills_manager
    if _skills_manager is None:
        _skills_manager = SkillsManager()
    return _skills_manager


def load_skills():
    """加载所有 Skills"""
    manager = get_skills_manager()
    manager.load_all()
    return manager
