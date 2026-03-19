# Baostock API 参考

## 模块导入

```python
import baostock as bs
```

## 登录/登出

```python
bs.login()    # 登录
bs.logout()   # 登出
```

使用匿名登录（无需账号密码）。

## 获取历史K线 (query_history_k_data_plus)

```python
rs = bs.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume,amount,pctChg",
    start_date='2024-01-01',
    end_date='2024-12-31',
    frequency="d",     # 日K
    adjustflag="2"     # 前复权
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| code | str | 股票代码 |
| fields | str | 返回字段，逗号分隔 |
| start_date | str | 开始日期 (yyyy-MM-dd) |
| end_date | str | 结束日期 (yyyy-MM-dd) |
| frequency | str | K线类型: `d`=日, `w`=周, `m`=月, `5/15/30/60`=分钟 |
| adjustflag | str | 复权类型: `1`=后复权, `2`=前复权, `3`=不复权 |

### 返回字段

| 字段 | 说明 |
|------|------|
| date | 日期 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 收盘价 |
| preclose | 昨收价 |
| volume | 成交量 |
| amount | 成交额 |
| adjustflag | 复权类型 |
| turn | 换手率 |
| tradestatus | 交易状态 |
| pctChg | 涨跌幅 (%) |
| peTTM | 市盈率TTM |
| pbMRQ | 市净率 |
| psTTM | 市销率TTM |
| pcfNcfTTM | 市现率TTM |
| isST | 是否ST |

### 遍历数据

```python
rs = bs.query_history_k_data_plus(...)
data_list = []
while rs.next():
    data_list.append(rs.get_row_data())

df = pd.DataFrame(data_list, columns=rs.fields)
```

### 返回码检查

```python
if rs.error_code != '0':
    logger.error(f"查询失败: {rs.error_msg}")
```

## A股代码格式

| 市场 | 格式 | 示例 |
|------|------|------|
| 上海主板 | sh.6xxxxx | sh.600000 |
| 深圳主板 | sz.0xxxxx | sz.000001 |
| 创业板 | sz.3xxxxx | sz.300001 |

### ETF 代码格式

**Baostock 支持 ETF 代码！** 经测试可用：

| ETF | Baostock格式 | 说明 |
|-----|-------------|------|
| 512050 | sh.512050 | 上海ETF |
| 159915 | sz.159915 | 深交所创业板ETF |
| 511880 | sh.511880 | 货币ETF |

**注意**: 创业板ETF代码 (159xxx) 使用 `sz.` 前缀。

### 代码标准化函数参考

```python
def symbol_to_baostock(symbol: str) -> str:
    """6位代码 → Baostock格式"""
    symbol = str(symbol).strip().zfill(6)
    # 上海: 6开头, 5开头(上海ETF), 9开头
    if symbol.startswith(('6', '5', '9')):
        return f"sh.{symbol}"
    # 深圳: 0开头, 1开头(深圳ETF/LOF), 3开头(创业板)
    return f"sz.{symbol}"
```

## 与 Futu API 对比

| 特性 | Baostock | Futu (futu-api) |
|------|---------|-----------------|
| 历史K线 | ✅ 支持股票+ETF | ✅ 支持股票+ETF (新SDK) |
| 实时行情 | ❌ 不支持 | ✅ 实时报价 |
| 登录方式 | 匿名无需账号 | 需要 Futu OpenD |
| 数据延迟 | T日收盘后 | 实时 |

## 复权类型映射

| 参数 | Baostock | Futu |
|------|----------|------|
| 前复权 | `2` | `AuType.QFQ` |
| 后复权 | `1` | `AuType.HFQ` |
| 不复权 | `3` | `AuType.NONE` |
