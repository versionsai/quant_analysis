# 量化选股推送服务 - 部署指南

## 功能介绍

- 每天定时推送股票买入信号
- 支持ETF/LOF和A股
- 包含止盈止损价位
- 通过Bark推送至iPhone

## 环境要求

- Docker
- Docker Compose

## 快速部署

### 1. 克隆项目到NAS

```bash
cd /root
git clone <你的Gitea仓库地址> quant_agent
cd quant_agent
```

### 2. 配置推送参数（可选）

```bash
cp .env.example .env
nano .env
```

如需 Bark 推送，修改 `BARK_KEY` 为你的 Bark Key；如需推送到微信，可额外配置 `SERVERCHAN_SENDKEY`：
```
BARK_KEY=你的BarkKey
SERVERCHAN_SENDKEY=你的Server酱SendKey
```

### 3. 一键部署

```bash
chmod +x deploy.sh
./deploy.sh init    # 初始化（首次运行）
./deploy.sh build   # 构建镜像
./deploy.sh start   # 启动服务
```

### 4. 查看状态

```bash
./deploy.sh status  # 查看运行状态
./deploy.sh logs    # 查看日志
```

## 部署命令说明

| 命令 | 说明 |
|------|------|
| `./deploy.sh init` | 初始化配置 |
| `./deploy.sh build` | 构建Docker镜像 |
| `./deploy.sh start` | 启动服务 |
| `./deploy.sh stop` | 停止服务 |
| `./deploy.sh restart` | 重启服务 |
| `./deploy.sh logs` | 查看日志 |
| `./deploy.sh status` | 查看状态 |
| `./deploy.sh push` | 手动推送一次 |
| `./deploy.sh update` | 更新服务 |
| `./deploy.sh clean` | 清理环境 |

## 自动运行说明

部署完成后，默认会启动两个常驻容器：

- `quant-stock-bot`
  运行 `/Users/jinhaoran/Documents/west/quant_agent/docker_start.py`
- `quant-stock-dashboard`
  运行 `/Users/jinhaoran/Documents/west/quant_agent/dashboard.py`

两者都使用 `restart: unless-stopped`，因此服务器重启后会自动拉起。

## 当前自动调度

### 1. 股票池

股票池会在交易日以下时间自动刷新：

- 08:30
- 09:25
- 09:30
- 09:40
- 09:50
- 10:00
- 10:15
- 10:30
- 13:00
- 13:15
- 13:30
- 14:00
- 14:15
- 14:30
- 14:50
- 14:57

其中 08:30 批次会带 `pre_market_us_news` 标记，用于结合前夜美股和新闻做盘前筛选。

股票池刷新完成后，会自动回调刷新信号池，不再依赖信号池独立定时任务。

### 2. 信号池与自动买入

每次股票池刷新后会自动执行：

- 实时扫描
- 信号池刷新
- 推荐记录写入
- AI/规则自动买入

### 3. 行情缓存与看板缓存

`quant-stock-dashboard` 启动后会自动刷新：

- 行情缓存
- 持仓实时行情
- 择时参数试验缓存

### 4. 资讯与推送

- 外围简报 / 盘后简报：按 `MARKET_BRIEF_PUSH_TIMES`
- 新闻报告：按 `NEWS_REPORT_TIME`
- 盘中诱多 / 诱空：按 `INTRADAY_TRAP_PUSH_TIMES`

### 5. 自动优化

- 每个交易日 `15:30` 自动执行策略优化

可在 `.env` 中修改:
```
PUSH_TIME_MORNING=09:28
PUSH_TIME_AFTERNOON=13:10
PUSH_TIME_CLOSE=15:30
INTRADAY_TRAP_PUSH_TIMES=09:45,10:00,10:30,10:45,11:30,13:15,13:45,14:15,14:30,14:45,15:00
```

## 目录结构

```
quant_agent/
├── .env                 # 配置文件 (需要创建)
├── .env.example         # 配置示例
├── Dockerfile           # Docker镜像配置
├── docker-compose.yml   # Docker Compose配置
├── deploy.sh           # 部署脚本
├── docker_start.py      # Docker启动入口
├── main.py             # 主程序
├── requirements.txt    # Python依赖
└── ...
```

## 常见问题

### 1. 推送失败

检查日志:
```bash
./deploy.sh logs
```

### 2. 数据获取失败

如果akshare无法访问，系统会自动使用baostock作为备选。

### 3. 修改推送时间

编辑 `.env` 文件后重启:
```bash
./deploy.sh restart
```

## CI/CD自动部署 (可选)

如果使用Gitea CI/CD，推送代码到main分支后将自动构建和部署。

需要配置的Secrets:
- `NAS_USER`: NAS登录用户名
- `NAS_PASSWORD`: NAS登录密码  
- `REGISTRY_USER`: 镜像仓库用户名 (可选)
- `REGISTRY_PASSWORD`: 镜像仓库密码 (可选)

## 股票池

默认监控:
- ETF/LOF: 银华日利、纳指、中概互联网、沪深300等
- A股: 贵州茅台、招商银行、宁德时代、比亚迪等

可在 `trading/realtime_monitor.py` 中修改股票池。
