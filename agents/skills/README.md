# Agents Skills 说明

本目录用于存放项目内 AI Agent 的运行时 Skills 配置。

适用对象：
- `agents/quant_agent.py`
- 未来需要读取 Agent prompt、信号参数、风控规则的项目代码

当前加载方式：
- 由 `agents/skills/manager.py` 从 `agents/skills/config/*.yaml` 加载
- 支持本地 YAML + 远程 Gitea 配置覆盖

配置职责：
- `config/agent.yaml`：Agent 系统提示词
- `config/signal.yaml`：信号相关参数
- `config/risk.yaml`：风控规则

设计原则：
- 这里存放“程序可读取”的结构化配置
- 不放操作手册、安装步骤、排障文档

与项目根目录 `skills/` 的区别：
- `agents/skills/`：给项目内 AI Agent 运行时使用
- `skills/`：给开发助手/维护者使用的说明文档
