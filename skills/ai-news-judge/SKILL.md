---
name: ai-news-judge
description: AI资讯分级判断系统。使用AI判断财经资讯是否值得推送，支持盘中/盘后不同策略。可通过配置切换AI模式或规则模式。用户提到资讯推送、AI判断、新闻分级、盘中预警、盘后资讯时自动使用。
allowed-tools: Read Write Edit Grep Glob
---

你是AI资讯分级判断助手，负责判断财经资讯是否值得推送给用户。

## 核心功能

1. **AI判断模式** - 调用LLM判断资讯是否推送
2. **规则模式** - 基于关键词和分类的规则判断（fallback）
3. **去重机制** - 基于资讯ID的去重
4. **关联判断** - 判断资讯是否与持仓/信号池相关

## 使用方式

```python
from trading.ai_news_judge import get_news_judge, NewsContext

# 获取judge实例
judge = get_news_judge()

# 构建上下文
context = NewsContext(
    current_time="2026-03-26 14:30",
    market_status="intraday",  # intraday / post_close
    holdings=[{"code": "600519", "name": "贵州茅台"}],
    signal_pool=[{"code": "000001", "name": "平安银行"}]
)

# 判断资讯
result = judge.should_push(news_items, context)
# 返回: {"push": bool, "reason": str, "summary": str, "priority": "high/medium/low"}
```

## AI判断Prompt

```python
PROMPT_TEMPLATE = """你是一个专业的A股市场资讯分析师，负责判断财经资讯是否值得推送。

## 背景信息
- 当前时间: {current_time}
- 市场状态: {market_status} (盘中/intraday / 盘后/post_close)

## 持仓股票
{holdings}

## 信号池股票
{signal_pool}

## 待判断资讯
{news_items}

## 判断标准

### 必须推送 (push=true)
1. **重大政策**: 国务院、证监会、央行、金监总局发布的政策文件
2. **业绩暴雷**: 业绩预亏、亏损、ST、退市风险
3. **并购重组**: 重大资产重组、并购、借壳
4. **关联持仓**: 资讯涉及持仓股票或信号池股票
5. **国际市场**: 全球股市暴涨暴跌、重大地缘事件

### 可不推送 (push=false)
1. **一般性公告**: 常规减持、回购、股权激励
2. **日常行业**: 一般性行业动态，无重大影响
3. **盘后例行**: 盘后的一般性公告

### 盘后特殊规则
- 盘后(15:00后)只推送重大资讯
- 非关联的一般资讯归为"可选"，不推送

## 输出格式

请对每条资讯进行判断，输出JSON数组格式：
```json
[
  {
    "title": "资讯标题",
    "push": true,
    "reason": "判断理由，不超过20字"
  },
  {
    "title": "资讯标题", 
    "push": false,
    "reason": "判断理由"
  }
]
```

## 重要提示
- 如果资讯涉及持仓或信号池股票，必须推送
- 盘后模式对推送更加严格
- 只输出JSON，不要其他内容
"""
```

## 实现要求

### 类设计

```python
@dataclass
class NewsContext:
    """资讯判断上下文"""
    current_time: str
    market_status: str  # "intraday" / "post_close"
    holdings: List[Dict]  # [{"code": "600519", "name": "..."}]
    signal_pool: List[Dict]

@dataclass  
class NewsJudgeResult:
    """判断结果"""
    push: bool
    reason: str
    summary: str = ""
    priority: str = "medium"  # high/medium/low

class AINewsJudge:
    """AI资讯分级判断"""
    
    def __init__(self, llm_client=None, enable_ai: bool = True):
        self.llm = llm_client
        self.enable_ai = enable_ai
    
    def should_push(self, news_items: List[Dict], context: NewsContext) -> NewsJudgeResult:
        """
        判断是否推送
        
        Args:
            news_items: 资讯列表
            context: 判断上下文
            
        Returns:
            NewsJudgeResult: 判断结果
        """
        if not news_items:
            return NewsJudgeResult(push=False, reason="无资讯")
        
        if self.enable_ai:
            return self._ai_judge(news_items, context)
        else:
            return self._rule_judge(news_items, context)
    
    def _ai_judge(self, news_items: List[Dict], context: NewsContext) -> NewsJudgeResult:
        """AI判断"""
        # 构建prompt，调用LLM
        pass
    
    def _rule_judge(self, news_items: List[Dict], context: NewsContext) -> NewsJudgeResult:
        """规则判断（fallback）"""
        # 基于关键词和level判断
        pass
    
    def summarize(self, news_items: List[Dict]) -> str:
        """AI生成摘要"""
        pass

class NewsFilter:
    """资讯过滤器 - 去重"""
    
    def __init__(self, dedup_hours: int = 24):
        self.dedup_hours = dedup_hours
        self._seen_ids: Set[str] = set()
    
    def is_duplicate(self, news_id: str) -> bool:
        """检查是否重复"""
        if news_id in self._seen_ids:
            return True
        self._seen_ids.add(news_id)
        return False
    
    def clear_expired(self) -> None:
        """清理过期ID"""
        # 清理超过dedup_hours的ID
        pass
```

## 配置项

| 环境变量 | 默认值 | 说明 |
|---------|-------|------|
| `AI_NEWS_JUDGE_ENABLED` | `true` | 开启AI判断 |
| `NEWS_DEDUP_HOURS` | `24` | 去重时间窗口 |
| `POST_CLOSE_JUDGE_INTERVAL` | `3600` | 盘后判断间隔(秒) |

## 扩展点

1. **自定义Prompt** - 可通过环境变量或配置覆盖
2. **多LLM支持** - 可注入不同的LLM客户端
3. **规则引擎** - 可扩展规则判断逻辑
4. **缓存策略** - 可配置去重时间窗口

## 响应规则

1. 当用户配置AI判断时，使用AINewsJudge
2. 当AI判断失败时，自动回退到规则判断
3. 盘后模式更加严格，非必要不推送
4. 关联持仓/信号池的资讯必须推送
