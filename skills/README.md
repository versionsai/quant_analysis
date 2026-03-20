# Skills 目录说明

本目录用于存放给开发助手/运维助手使用的协作型 Skills 文档。

适用对象：
- Codex、ChatGPT、运维脚本助手
- 项目维护人员

主要用途：
- 记录外部系统接入步骤
- 说明本地开发/排障流程
- 提供 OpenD、OpenAPI、部署等操作手册

约束：
- 本目录内容默认不会被项目运行时代码直接加载
- 这里的文档用于“指导人或助手如何操作”，而不是直接驱动业务逻辑

与 `agents/skills/config/` 的区别：
- `skills/`：面向协作和操作说明
- `agents/skills/config/`：面向项目内 AI Agent 的运行时结构化配置

当前已存在示例：
- `futu/openapi.md`：Futu OpenAPI / OpenD 接入说明
- `futu/install-opend.md`：OpenD 安装与连接检查
- `futu/market-data.md`：Futu 行情与五档盘口说明
- `futu/trade.md`：Futu 交易能力边界与接入前置条件
- `openapi/README.md`：外部 OpenAPI Skills 入口说明
