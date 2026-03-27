# 量化交易、监控与看板系统

这是一个面向 A 股场景的量化交易项目，当前已经不是单纯的“回测脚本集合”，而是一套包含回测、实时监控、热点策略、推送、复盘和看板的完整工作流。

当前项目重点能力包括：

- ETF/LOF 与股票池管理
- 策略回测与批量比较
- 盘中实时扫描与预警
- TACO / TACO-OIL 热点驱动策略
- 推荐记录、运行时复盘与报告
- 看板展示与操作入口
- Bark / Server酱 等推送链路
- NAS Docker 部署与 Gitea 自动发布

## 当前项目结构

核心入口和关键目录如下：

```text
sai/
|-- agents/                # AI Agent 与工具
|-- backtest/              # 回测引擎与绩效分析
|-- config/                # 配置、事件日历、运行时参数
|-- dashboard/             # 看板前端页面
|-- data/                  # 数据源、股票池、缓存
|-- runtime/               # 运行时数据库、报告、临时产物
|-- skills/                # 项目级技能文档
|-- strategy/              # 策略实现
|-- trading/               # 实时监控、复盘、推送、执行辅助
|-- utils/                 # 日志与工具函数
|-- main.py                # 主 CLI 入口
|-- dashboard.py           # 看板服务
|-- docker_start.py        # 定时推送与主服务启动
`-- taco_compare.py        # TACO 参数批量对比工具
```

重点文件：

- [main.py](D:/SAI_PROJECT/quant_agent/sai/main.py)
- [dashboard.py](D:/SAI_PROJECT/quant_agent/sai/dashboard.py)
- [dashboard/index.html](D:/SAI_PROJECT/quant_agent/sai/dashboard/index.html)
- [docker_start.py](D:/SAI_PROJECT/quant_agent/sai/docker_start.py)
- [strategy/examples/taco_strategy.py](D:/SAI_PROJECT/quant_agent/sai/strategy/examples/taco_strategy.py)
- [taco_compare.py](D:/SAI_PROJECT/quant_agent/sai/taco_compare.py)

## 环境准备

建议 Python 3.10+。

安装依赖：

```bash
pip install -r requirements.txt
```

当前依赖包含的主要能力：

- 行情与数据：`akshare`、`baostock`、`yfinance`、`jqdatasdk`
- 量化分析：`pandas`、`numpy`、`scipy`、`stockstats`
- AI 能力：`deepagents`、`langchain-openai`、`langgraph`
- 看板与配置：`python-dotenv`、`pyyaml`
- 交易接口：`futu-api`

## 快速开始

先看帮助：

```bash
python main.py --help
```

常用本地命令：

```bash
python main.py --mode pool-update
python main.py --mode backtest --strategy pa_macd
python main.py --mode backtest --strategy taco
python main.py --mode compare
python main.py --mode realtime --once
python main.py --mode review
python main.py --mode taco-compare
python main.py --mode taco-monitor --strategy taco
python dashboard.py
python docker_start.py
```

## 主入口说明

[main.py](D:/SAI_PROJECT/quant_agent/sai/main.py) 当前支持的 `--mode`：

- `pool`
- `backtest`
- `compare`
- `realtime`
- `pool-update`
- `weak-strong`
- `emotion-scan`
- `review`
- `taco-compare`
- `taco-monitor`

当前支持的 `--strategy`：

- `pa_macd`
- `macd`
- `pa`
- `breakout`
- `weak_strong`
- `taco`
- `taco_oil`

补充说明：

- `taco` 和 `taco_oil` 当前优先扫描 ETF/LOF。
- 其他策略仍然以股票池为主，除非代码里明确调整。

## TACO / TACO-OIL

[strategy/examples/taco_strategy.py](D:/SAI_PROJECT/quant_agent/sai/strategy/examples/taco_strategy.py) 当前实现的是一类“热点驱动 + 事件修复”策略，而不是传统固定窗口策略。

当前特征：

- 跟踪特朗普言论、关税、贸易摩擦、石油、中东、AI、芯片、防务、供应链等热点
- 使用最近 30 天事件窗口
- 结合新闻、关键词、事件分数和价格修复结构
- 输出 `event_score`、`threshold`、`window_days`、`reason`、`matched_keywords`
- 候选标的优先面向 ETF/LOF，适合热点主题和外围映射产品

相关能力：

- `python main.py --mode taco-monitor --strategy taco`
- `python main.py --mode taco-monitor --strategy taco_oil`
- `python main.py --mode taco-compare`

[taco_compare.py](D:/SAI_PROJECT/quant_agent/sai/taco_compare.py) 还支持自定义区间、标的、阈值和关键词权重，适合做参数批量比较。

## 看板

看板后端在 [dashboard.py](D:/SAI_PROJECT/quant_agent/sai/dashboard.py)，前端页面在 [dashboard/index.html](D:/SAI_PROJECT/quant_agent/sai/dashboard/index.html)。

本地启动：

```bash
python dashboard.py
```

默认地址：

```text
http://127.0.0.1:18675
```

当前看板主要包含：

- 总览统计
- 市场模式
- 最新动态
- 当前持仓
- 信号池
- 实时行情卡片
- 功能状态
- TACO Diagnostics
- TACO Hot Topics

其中 TACO 相关面板会展示：

- 当前是否激活
- 事件分数
- 阈值
- 窗口长度
- 激活原因
- 命中关键词
- 热点分组与来源

## 实时监控与推送

[docker_start.py](D:/SAI_PROJECT/quant_agent/sai/docker_start.py) 负责定时推送主流程，当前已经包含：

- 盘前 / 盘后简报
- 盘中预警
- AI 增强资讯提炼
- 持仓、信号与复盘拼装
- Bark 推送
- Server酱推送

[trading/realtime_monitor.py](D:/SAI_PROJECT/quant_agent/sai/trading/realtime_monitor.py) 负责实时监控扫描。

推荐本地验证命令：

```bash
python main.py --mode realtime --once
python docker_start.py
```

## 数据与运行时文件

常见目录和文件：

- `./runtime/data/recommend.db`：运行时数据库
- `./runtime/`：运行期报告、缓存、结果
- `./logs/`：日志输出
- `./data/`：数据源、股票池、缓存辅助

如果运行链路依赖数据库，请优先确认：

- `recommend.db` 是否存在
- 相关表是否已初始化
- 本地数据源是否可连通

## Windows 中文编码注意事项

这个项目会在 Windows 下频繁运行，中文乱码是高频问题。

当前已经在 [utils/logger.py](D:/SAI_PROJECT/quant_agent/sai/utils/logger.py) 中加入 Windows UTF-8 控制台处理，但仍然建议遵循下面规则：

- 源码文件统一使用 UTF-8
- 不要把终端里看到的乱码直接复制回代码
- 修改中文日志、HTML、Markdown、JSON 后，要在 Windows 终端真实验证
- 如果任务涉及乱码修复，先看 [skills/windows-utf8-guard/SKILL.md](D:/SAI_PROJECT/quant_agent/sai/skills/windows-utf8-guard/SKILL.md)

建议的最小验证：

```bash
python main.py --help
python -c "from utils.logger import get_logger; print('中文输出测试'); logger=get_logger('encoding_test'); logger.info('中文日志测试')"
```

## Docker 与部署

项目已经支持 Docker 运行和 Gitea 自动部署。

本地或 NAS Docker 启动：

```bash
docker compose up -d --build
```

当前部署相关重点：

- 工作流文件：`D:\SAI_PROJECT\quant_agent\sai\.gitea\workflows\deploy.yml`
- push 到 `main` / `master` 后可触发自动部署
- 看板服务默认暴露 `18675`

如果部署在 NAS：

```text
http://NAS_IP:18675
```

## Gitea Secrets / Variables 建议

常用 Secrets：

- `NAS_HOST`
- `NAS_PORT`
- `NAS_USER`
- `NAS_PASSWORD`
- `NAS_PROJECT_DIR`
- `BARK_KEY`
- `SERVERCHAN_SENDKEY`
- `SILICONFLOW_API_KEY`
- `FUTU_HOST`
- `FUTU_PORT`

常用 Variables：

- `SILICONFLOW_MODEL`
- `NEWS_REPORT_TIME`
- `PUSH_TIME_MORNING`
- `PUSH_TIME_AFTERNOON`
- `PUSH_TIME_CLOSE`
- `INTRADAY_TRAP_PUSH_TIMES`
- `LOG_LEVEL`

## 常见验证命令

一般改动后：

```bash
python -m py_compile main.py dashboard.py taco_compare.py
python main.py --help
```

策略改动后：

```bash
python -m py_compile strategy/examples/taco_strategy.py strategy/__init__.py strategy/examples/__init__.py
python main.py --mode taco-compare
```

看板改动后：

```bash
python -m py_compile dashboard.py
python dashboard.py
```

实时链路改动后：

```bash
python main.py --mode taco-monitor --strategy taco
python main.py --mode taco-monitor --strategy taco_oil
python main.py --mode realtime --once
```

Windows 中文改动后：

```bash
python main.py --help
python -c "from utils.logger import get_logger; print('中文输出测试'); logger=get_logger('encoding_test'); logger.info('中文日志测试')"
```

## 相关文档

- [AGENTS.md](D:/SAI_PROJECT/quant_agent/sai/AGENTS.md)：给 AI Agent 的仓库工作规范
- [DEPLOY.md](D:/SAI_PROJECT/quant_agent/sai/DEPLOY.md)：部署相关说明
- [skills/windows-utf8-guard/SKILL.md](D:/SAI_PROJECT/quant_agent/sai/skills/windows-utf8-guard/SKILL.md)：Windows 中文编码处理规范
- [docs/PROJECT_SYSTEM_GUIDE.md](D:/SAI_PROJECT/quant_agent/sai/docs/PROJECT_SYSTEM_GUIDE.md)：当前项目完整运行与策略说明

## 当前维护原则

- TACO 策略路径保持 ETF/LOF 优先
- 其他策略默认仍以股票为主
- 看板必须能解释“为什么激活、为什么不激活”
- Windows 中文输出保持可读
- 避免因为远程池获取而拖慢本地实时扫描
