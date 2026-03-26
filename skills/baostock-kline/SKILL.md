---
name: baostock-kline
description: Baostock 历史K线数据使用规范。所有历史K线数据必须使用 baostock 获取，不允许使用 futu、akshare 或其他数据源。用户提到 K线、历史数据、复权数据、get_kline、history_k_line 时自动使用。
allowed-tools: Read Grep Glob
---

你是本项目的历史K线数据规范助手，负责告知 AI Agent 所有历史K线数据必须从 baostock 获取。

## 核心规则

**所有历史K线数据必须使用 baostock，不允许使用其他数据源。**

## 原因

1. **baostock 完全免费**：无需 API 密钥，无调用限制
2. **数据质量可靠**：支持股票+ETF，支持前复权/后复权
3. **项目统一数据源**：避免多数据源造成的数据不一致

## 正确的实现方式

### 使用 DataSource.get_kline()

项目中的 `data/data_source.py` 已封装好 baostock 调用：

```python
from data import DataSource

ds = DataSource()
# 股票K线
df = ds.get_kline("600519", "20250101", "20250326", adjust="qfq")
# ETF K线
df = ds.get_kline("512050", "20250101", "20250326", adjust="qfq")
```

**参数说明：**
- `symbol`: 6位代码，如 "600519", "512050"
- `start_date`: 开始日期，YYYYMMDD 格式
- `end_date`: 结束日期，YYYYMMDD 格式
- `adjust`: 复权类型，qfq(前复权) / hfq(后复权) / None(不复权)

### 直接调用 baostock

```python
import baostock as bs

# 登录
bs.login()

# 查询
rs = bs.query_history_k_data_plus(
    "sh.600519",
    "date,open,high,low,close,volume,amount,pctChg",
    start_date="2025-01-01",
    end_date="2025-03-26",
    frequency="d",
    adjustflag="2"  # 2=前复权
)

# 处理数据
data_list = []
while rs.next():
    data_list.append(rs.get_row_data())

df = pd.DataFrame(data_list, columns=rs.fields)

# 登出
bs.logout()
```

## 禁止使用的数据源

以下数据源**禁止**用于获取历史K线：

| 数据源 | 原因 |
|--------|------|
| futu / futu-api | 有额度限制，仅用于实时行情 |
| akshare | 数据不稳定，不作为主数据源 |
| tushare | 需要token，有调用限制 |

## 常见错误

### 错误1：使用 futu 获取历史K线

```python
# ❌ 错误：使用 futu 获取历史K线
from futu import OpenQuoteContext
ctx = OpenQuoteContext(host="127.0.0.1", port=11111)
ret, data = ctx.get_history_kline("SH.600519", start_date="2025-01-01", end_date="2025-03-26")
```

**正确做法：**
```python
# ✅ 正确：使用 baostock
from data import DataSource
ds = DataSource()
df = ds.get_kline("600519", "20250101", "20250326")
```

### 错误2：混合使用多个数据源

```python
# ❌ 错误：尝试 fallback 到其他数据源
try:
    df = ds.get_kline(...)  # baostock
except Exception:
    df = futu_ctx.get_history_kline(...)  # 禁止 fallback 到 futu
```

**正确做法：**
```python
# ✅ 正确：仅使用 baostock，如果失败则返回空DataFrame
try:
    df = ds.get_kline(...)  # baostock
except Exception as e:
    logger.error(f"Baostock获取K线失败: {e}")
    return pd.DataFrame()
```

## 查找现有代码中的问题

使用 Grep 工具检查项目中是否有违规使用：

```bash
# 检查是否使用了 futu 获取历史K线
grep -r "get_history_kline\|get_kline.*futu" --include="*.py"
```

如果发现违规使用，报告给用户并建议修改。

## 响应规则

1. 当用户问起历史K线数据来源时，明确告知必须使用 baostock
2. 当发现代码中使用了其他数据源获取历史K线时，指出问题并提供正确做法
3. 不要主动修改代码，只需告知正确的实现方式
