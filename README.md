# 量化交易与推送系统

这是一个面向 A 股场景的量化交易与推送项目，当前能力已经覆盖：

- 动态股票池
- 信号池与持仓管理
- 实时监控与盘中预警
- Bark 推送
- AI Agent 分析
- 妙想 / 财联社 / Futu 数据接入
- NAS Docker 自动部署

## 本地运行

安装依赖：

```bash
pip install -r requirements.txt
```

常用命令：

```bash
python main.py --mode pool-update
python main.py --mode realtime
python docker_start.py
python dashboard.py
```

看板默认地址：

```bash
http://127.0.0.1:18675
```

如果部署在 NAS Docker 中：

```bash
http://NAS_IP:18675
```

## Docker 服务

当前 `docker-compose.yml` 中包含两个核心服务：

- `quant-bot`：定时推送、盘中预警、AI 分析主服务
- `quant-dashboard`：页面看板服务，暴露端口 `18675`

部署或本地 Docker 运行：

```bash
docker compose up -d --build
```

## CI/CD 自动部署

项目已经配置了 Gitea Actions 工作流：

- 工作流文件：`D:\SAI_PROJECT\quant_agent\sai\.gitea\workflows\deploy.yml`
- 触发条件：push 到 `main` / `master`
- 当前行为：**自动 SSH 到 NAS，并在 NAS 上执行 `docker compose up -d --build --remove-orphans`**

也就是说，现在正常情况下：

1. 代码 push 到远端
2. Gitea Runner 自动执行部署
3. NAS 自动更新 `quant-bot` 和 `quant-dashboard`
4. 不需要再手动登录 NAS 执行 `docker compose up -d`

## Gitea 必填 Secrets

建议在 Gitea 中配置这些 Secrets：

- `NAS_HOST`
- `NAS_PORT`
- `NAS_USER`
- `NAS_PASSWORD`
- `NAS_PROJECT_DIR`
- `BARK_KEY`
- `SILICONFLOW_API_KEY`
- `EM_API_KEY`
- `FUTU_HOST`
- `FUTU_PORT`

## 建议配置的 Variables

- `SILICONFLOW_MODEL`
- `NEWS_REPORT_TIME`
- `PUSH_TIME_MORNING`
- `PUSH_TIME_AFTERNOON`
- `PUSH_TIME_CLOSE`
- `INTRADAY_TRAP_PUSH_TIMES`
- `LOG_LEVEL`

## 部署前置条件

要让自动部署真正生效，NAS 侧需要满足：

- Gitea Runner 可以通过 SSH 访问 NAS
- NAS 已安装 `docker compose` 或 `docker-compose`
- `NAS_PROJECT_DIR` 对应目录可写
- 部署用户可以执行 `sudo docker ...`

## CI/CD 部署后检查清单

每次推送到 `main` 后，如果 Gitea Workflow 成功，建议按下面顺序快速验收：

### 1. 检查容器状态

如果 NAS 上有 Compose：

```bash
docker compose ps
```

如果 NAS 上没有 Compose，也可以直接看容器：

```bash
docker ps | grep quant-stock-
```

正常情况下应至少看到：

- `quant-stock-bot`
- `quant-stock-dashboard`

### 2. 检查看板是否可访问

浏览器打开：

```bash
http://NAS_IP:18675
```

如果不能访问，重点检查：

- `quant-stock-dashboard` 容器是否存在
- NAS 防火墙是否放行 `18675`
- 路由器 / 反向代理是否拦截该端口

### 3. 检查主服务日志关键词

查看主服务日志：

```bash
docker logs quant-stock-bot --tail 200
```

优先关注这些关键词：

- `AI Agent 初始化成功`
- `Futu连接成功`
- `股票池更新完成`
- `推送成功`

如果看到下面这些内容，说明对应能力有问题：

- `Futu连接失败`
- `EM_API_KEY is not set`
- `SILICONFLOW_API_KEY is required`
- `盘中预警 AI 研判失败`

### 4. 检查看板日志关键词

```bash
docker logs quant-stock-dashboard --tail 200
```

优先关注：

- `看板服务启动`
- `GET /api/overview HTTP/1.1" 200`
- `GET /api/market HTTP/1.1" 200`

### 5. 检查 Futu 是否恢复正常

如果此前出现过：

```bash
Futu连接失败: No module named 'futuquant'
```

在当前版本之后，重新构建镜像应安装 `futu-api`。  
正常日志应更接近：

- `Futu连接成功: 192.168.x.x:11111 (futu-api)`

### 6. 检查妙想与 AI 配置是否注入

如果需要核对配置是否真正进了容器，可在 NAS 上执行：

```bash
docker exec -it quant-stock-bot env | grep -E "EM_API_KEY|SILICONFLOW_API_KEY|ENABLE_AI_AGENT|FUTU_HOST|FUTU_PORT"
```

### 7. 检查推送链路

如果 Bark 推送没到手机，优先看：

```bash
docker logs quant-stock-bot --tail 200 | grep Bark
```

正常应看到类似：

- `Bark推送成功`

### 8. 一句话验收标准

本项目当前一轮 CI/CD 成功后的理想状态是：

- `quant-stock-bot` 正常运行
- `quant-stock-dashboard` 正常运行
- `http://NAS_IP:18675` 可访问
- 日志里出现 `Futu连接成功`
- 日志里没有 `EM_API_KEY is not set`
- Bark 能正常收到推送

## 看板功能

当前页面已经支持：

- 总览统计
- 功能状态检查
- 当前持仓
- 信号池
- 最近荐股
- 交易事件时间线
- 最近日志
- 盘中实时行情卡片（指数 / ETF / 持仓涨跌）
- 操作页：
  - 刷新股票池
  - 触发一次推送
  - 触发盘中预警

## 相关文件

- 主部署工作流：`D:\SAI_PROJECT\quant_agent\sai\.gitea\workflows\deploy.yml`
- Docker 编排：`D:\SAI_PROJECT\quant_agent\sai\docker-compose.yml`
- 定时推送入口：`D:\SAI_PROJECT\quant_agent\sai\docker_start.py`
- 仪表盘服务：`D:\SAI_PROJECT\quant_agent\sai\dashboard.py`
- 仪表盘页面：`D:\SAI_PROJECT\quant_agent\sai\dashboard\index.html`
