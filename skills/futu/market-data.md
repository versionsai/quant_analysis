# Futu Market Data Skill

本 Skill 用于说明本项目里如何通过 Futu 获取实时行情、分时、五档盘口和经纪队列。

## 本项目当前已接入能力

`data/data_source.py` 当前实际接入：
- `get_market_snapshot`
- `get_rt_data`
- `get_cur_kline(..., K_1M)`

当前已验证但尚未正式封装：
- `get_order_book`

当前可调用但不建议视为稳定数据源：
- `get_broker_queue`

## 已验证结论

基于 `.env.local` 对应的 OpenD 实测：

- A 股实时快照：可用
- A 股分时数据：可用
- A 股五档盘口：可用
- A 股经纪队列：接口返回成功，但当前为空表

## 标准验证命令

### 1. 实时快照

```powershell
python -c "from dotenv import dotenv_values; from futu import OpenQuoteContext; cfg=dotenv_values('.env.local'); q=OpenQuoteContext(host=cfg['FUTU_HOST'], port=int(cfg['FUTU_PORT'])); ret, data=q.get_market_snapshot(['SH.600036']); print(ret); print(data[['code','last_price','volume']].head().to_string(index=False)); q.close()"
```

### 2. 分时数据

```powershell
python -c "from dotenv import dotenv_values; from futu import OpenQuoteContext, SubType; cfg=dotenv_values('.env.local'); q=OpenQuoteContext(host=cfg['FUTU_HOST'], port=int(cfg['FUTU_PORT'])); q.subscribe(['SH.600036'], [SubType.RT_DATA], subscribe_push=False); ret, data=q.get_rt_data('SH.600036'); print(ret); print(data.tail(3).to_string(index=False)); q.close()"
```

### 3. 五档盘口

```powershell
python -c "from dotenv import dotenv_values; from futu import OpenQuoteContext, SubType; cfg=dotenv_values('.env.local'); q=OpenQuoteContext(host=cfg['FUTU_HOST'], port=int(cfg['FUTU_PORT'])); q.subscribe(['SH.600036'], [SubType.ORDER_BOOK], subscribe_push=False); ret, data=q.get_order_book('SH.600036', num=5); print(ret); print(data['Bid']); print(data['Ask']); q.close()"
```

### 4. 经纪队列

```powershell
python -c "from dotenv import dotenv_values; from futu import OpenQuoteContext, SubType; cfg=dotenv_values('.env.local'); q=OpenQuoteContext(host=cfg['FUTU_HOST'], port=int(cfg['FUTU_PORT'])); q.subscribe(['SH.600036'], [SubType.BROKER], subscribe_push=False); ret, bid, ask=q.get_broker_queue('SH.600036'); print(ret); print(bid.head().to_string(index=False)); print(ask.head().to_string(index=False)); q.close()"
```

## 使用提醒

- `get_rt_data` 前必须先订阅 `SubType.RT_DATA`
- `get_order_book` 前必须先订阅 `SubType.ORDER_BOOK`
- `get_broker_queue` 前必须先订阅 `SubType.BROKER`
- 五档盘口可以作为盘中信号增强数据
- 经纪队列在 A 股上不一定稳定返回

## 后续建议

如果要让项目正式消费五档盘口，建议新增统一封装：
- `data/data_source.py` -> `get_order_book(symbol: str, depth: int = 5)`
