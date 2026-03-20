# Futu OpenAPI / OpenD Skill

本文件是 `skills/futu/` 的总入口，用于说明富途官方 AI/Skills 能力与本项目本地化技能文档之间的关系。

## 推荐阅读顺序

1. `install-opend.md`
   - 安装、连接、OpenD 基础可用性检查
2. `market-data.md`
   - 实时快照、分时、五档盘口、经纪队列
3. `trade.md`
   - 交易能力边界、模拟盘/实盘约束、接入前置条件
4. 本文件
   - 官方技能背景、项目本地化说明、目录关系

## 官方文档要点

根据富途官方文档：

- OpenAPI 文档支持下载 Markdown，适合作为 AI 上下文输入
- 官方提供 OpenD Skills 编程技能包，包含两个技能模块：
  - `openapi`：行情交易助手
  - `install-opend`：OpenD 安装助手
- `openapi` 覆盖行情查询、交易操作、实时订阅，并附带 API 接口速查
- 使用前需要先手动登录 OpenD
- 交易默认使用模拟环境；实盘交易需要显式说明并二次确认
- 需要关注接口限频和订阅额度限制

## 本地化说明

本项目的 `skills/futu/openapi.md` 不是官方技能包原件，而是：

- 参考官方 AI/Skills 文档整理的项目内协作型 skill
- 用于帮助开发助手理解本项目的 Futu 接入方式
- 用于指导本项目内的 OpenD 连接、行情验证和排障

因此，本文件重点是：
- 保留官方推荐的使用方式和注意事项
- 补充本项目 `.env.local`、数据源封装和已验证能力

## 本项目本地化约定

- 本地运行优先使用 `.env.local`
- 当前 `.env.local` 中的 Futu 配置：
  - `FUTU_HOST=192.168.5.6`
  - `FUTU_PORT=11111`
- 运行时代码主要通过以下模块接入 Futu：
  - `data/data_source.py`
  - `futu_opend_manager.py`
  - `futu_http_server.py`

## 官方推荐的 AI 接入方式

如果你需要把富途 OpenAPI 能力提供给通用 AI 工具，官方建议两条路径：

### 1. 提供 Markdown 文档给 AI

- 从官方文档站下载 Markdown 版本
- 将文档放入项目目录，或直接作为上下文发给 AI
- 适合代码生成、接口问答、参数查阅

### 2. 安装官方 OpenD Skills 技能包

- 官方技能包下载地址：
  - `https://openapi.futunn.com/skills/opend-skills.zip`
- 官方技能包包含：
  - `openapi`
  - `install-opend`

适合场景：
- 让支持 Skills 的 AI 工具直接调用富途能力
- 快速完成 OpenD 安装、行情查询、交易示例生成

说明：
- 本项目当前保留的是“本地化协作 skill”
- 如果未来需要完整对齐官方技能包，可以把官方 `SKILL.md` 作为补充资料一起放入 `skills/futu/`

## 目录说明

- `openapi.md`
  - 总入口
- `install-opend.md`
  - OpenD 安装与连接检查
- `market-data.md`
  - 行情能力与五档盘口验证
- `trade.md`
  - 交易能力边界与接入前置条件

## 后续建议

如果项目要正式消费五档盘口，建议在 `data/data_source.py` 中新增：
- `get_order_book(symbol: str, depth: int = 5)`

这样 `RealtimeMonitor`、模拟交易和盘中策略就能直接复用同一层数据源封装。

## 官方参考

- 富途官方文档：
  - https://openapi.futunn.com/futu-api-doc/intro/ai.html
