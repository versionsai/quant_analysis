# Futu Install OpenD Skill

本 Skill 用于指导开发助手或维护者安装、配置和验证 Futu OpenD。

## 目标

- 确认 OpenD 已安装
- 确认 OpenD 已登录
- 确认本机或局域网可以访问 OpenD 端口
- 为本项目的 Futu 行情能力提供基础环境

## 官方安装来源

- OpenD 下载入口：
  - https://www.futunn.com/market-center/developers
- AI / Skills 参考文档：
  - https://openapi.futunn.com/futu-api-doc/intro/ai.html

## 本项目本地约定

- 本地运行优先使用 `.env.local`
- 当前 OpenD 配置：
  - `FUTU_HOST=192.168.5.6`
  - `FUTU_PORT=11111`

## 标准检查步骤

### 1. 检查端口连通性

```powershell
Test-NetConnection 192.168.5.6 -Port 11111
```

### 2. 检查 Python SDK

```powershell
python -c "from futu import OpenQuoteContext; print('ok')"
```

### 3. 检查 OpenD 连接

```powershell
python -c "from dotenv import dotenv_values; from futu import OpenQuoteContext; cfg=dotenv_values('.env.local'); q=OpenQuoteContext(host=cfg['FUTU_HOST'], port=int(cfg['FUTU_PORT'])); print('connected'); q.close()"
```

## 项目内相关文件

- `futu_opend_manager.py`
- `futu_http_server.py`
- `data/data_source.py`

## 常见问题

### 1. 端口不通

优先检查：
- OpenD 是否启动
- OpenD 是否完成登录
- 局域网访问是否被防火墙拦截
- `.env.local` 中的地址是否正确

### 2. Python SDK 缺失

```powershell
python -m pip install futu-api
```

### 3. 连接成功但接口无数据

优先检查：
- OpenD 当前账号是否登录
- 行情权限是否足够
- 是否已先完成对应类型的订阅
