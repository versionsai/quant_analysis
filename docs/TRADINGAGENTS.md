## 目的

集成 [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents)，为当前项目的 AI Agent 增加更丰富的辅助能力：

- 单股/ETF 深度分析（技术/基本面/新闻/情绪/风险）
- 大盘/指数情绪分析（用指数 ticker 代替大盘）

## 安装

本项目已在 `requirements.txt` 增加依赖：

```bash
pip install -r requirements.txt
```

## 环境变量

在 `.env`（参考 `.env.example`）中按需配置：

- `TRADINGAGENTS_LLM_PROVIDER`：模型提供方（默认使用 TradingAgents 内置默认值）
- `TRADINGAGENTS_BACKEND_URL`：OpenAI 兼容接口地址（例如 SiliconFlow）
- `TRADINGAGENTS_DEEP_MODEL`：深度模型
- `TRADINGAGENTS_QUICK_MODEL`：快速模型
- `TRADINGAGENTS_MAX_DEBATE_ROUNDS`：辩论轮数
- `TRADINGAGENTS_ONLINE_TOOLS`：是否启用联网工具（true/false）

还需要为对应提供方配置 API Key，例如：

- `OPENAI_API_KEY`
- `OPENROUTER_API_KEY`

如果你使用 SiliconFlow 的 OpenAI 兼容接口，建议：

```bash
export TRADINGAGENTS_LLM_PROVIDER=openai
export TRADINGAGENTS_BACKEND_URL=https://api.siliconflow.cn/v1
export OPENAI_API_KEY=$SILICONFLOW_API_KEY
```

## 如何使用（工具）

DeepAgents 会自动发现并调用以下工具（见 `agents/tools/tradingagents_tools.py`）：

- `ta_analyze_stock(symbol, trade_date="YYYY-MM-DD")`
- `ta_market_sentiment(index_symbol="000001", trade_date="YYYY-MM-DD")`

其中 A 股 6 位代码会自动映射为 ticker：

- `6xxxxxx/5xxxxxx/9xxxxxx` → `.SS`
- 其他 6 位数字 → `.SZ`
