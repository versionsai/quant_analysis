# 量化看板使用说明

新增了一个零额外依赖的简洁页面，用于查看：

- 当前持仓
- 信号池
- 最近荐股
- 交易时间线
- 功能状态（AI Agent、妙想、Bark、Futu、数据库）
- 最近日志

## 启动方式

```bash
python dashboard.py
```

默认访问地址：

```bash
http://127.0.0.1:18675
```

如果部署在 NAS 的 Docker 中，访问方式改为：

```bash
http://NAS_IP:18675
```

## Docker / NAS 部署说明

`docker-compose.yml` 已新增独立服务：

- `quant-bot`：定时推送主服务
- `quant-dashboard`：页面看板服务

看板容器会：

- 运行 `python dashboard.py --host 0.0.0.0`
- 映射端口 `18675:18675`
- 共享主服务的数据库和日志目录

更新后建议执行：

```bash
docker compose up -d --build
```

查看看板容器状态：

```bash
docker ps | grep quant-stock-dashboard
docker logs quant-stock-dashboard --tail 100
```

自定义数据库：

```bash
python dashboard.py --db-path ./runtime/data/recommend.db
```

## 页面特点

- 不依赖 Flask / FastAPI / Streamlit
- 使用项目现有 SQLite 数据
- 自动每 30 秒刷新一次
- 适合快速查看盘前、盘中、收盘状态
- 端口固定为 `18675`
- 支持手动触发：
  - 刷新股票池
  - 触发一次完整推送
  - 触发盘中预警
- 支持查看盘中实时行情卡片：
  - 指数
  - ETF
  - 持仓涨跌

## 可继续增强的方向

- 增加“只看持仓 / 只看信号池”的筛选
- 增加交易生命周期图表
- 增加操作执行日志和最近一次执行结果卡片
