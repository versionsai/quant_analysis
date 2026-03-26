# -*- coding: utf-8 -*-
"""
AI资讯分级判断系统
使用AI判断财经资讯是否值得推送，支持盘中/盘后不同策略
"""
import os
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set

from utils.logger import get_logger

logger = get_logger(__name__)


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
  {{"title": "资讯标题", "push": true, "reason": "判断理由，不超过20字"}},
  {{"title": "资讯标题", "push": false, "reason": "判断理由"}}
]
```

## 重要提示
- 如果资讯涉及持仓或信号池股票，必须推送
- 盘后模式对推送更加严格
- 只输出JSON，不要其他内容
"""


SUMMARIZE_PROMPT = """请将以下财经资讯压缩成3-5条中文要点，保留核心信息：

{news_items}

输出格式：每条一行，不需要编号
"""


@dataclass
class NewsContext:
    """资讯判断上下文"""
    current_time: str
    market_status: str  # "intraday" / "post_close"
    holdings: List[Dict] = field(default_factory=list)
    signal_pool: List[Dict] = field(default_factory=list)


@dataclass
class NewsJudgeResult:
    """判断结果"""
    push: bool
    reason: str
    summary: str = ""
    priority: str = "medium"


class AINewsJudge:
    """AI资讯分级判断"""
    
    def __init__(self, llm_client=None, enable_ai: bool = None):
        if enable_ai is None:
            enable_ai = os.environ.get("AI_NEWS_JUDGE_ENABLED", "true").lower() == "true"
        self.enable_ai = enable_ai
        self._llm = llm_client
    
    def _get_llm(self):
        """获取LLM客户端"""
        if self._llm is not None:
            return self._llm
        try:
            api_key = os.environ.get("SILICONFLOW_API_KEY", "")
            if not api_key:
                logger.warning("未配置SILICONFLOW_API_KEY")
                return None
            from agents.llm.siliconflow import SiliconFlowLLM
            return SiliconFlowLLM(api_key=api_key, temperature=0.3, max_tokens=2000)
        except Exception as e:
            logger.warning(f"获取LLM客户端失败: {e}")
            return None
    
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
        llm = self._get_llm()
        if llm is None:
            logger.warning("AI判断失败，回退到规则判断")
            return self._rule_judge(news_items, context)
        
        try:
            holdings_text = "\n".join([
                f"- {h.get('code', '')} {h.get('name', '')}"
                for h in context.holdings[:10]
            ]) or "无"
            
            signal_text = "\n".join([
                f"- {s.get('code', '')} {s.get('name', '')}"
                for s in context.signal_pool[:10]
            ]) or "无"
            
            news_text = "\n".join([
                f"- [{item.get('level', 'normal')}] {item.get('title', item.get('name', ''))}"
                for item in news_items[:10]
            ])
            
            prompt = PROMPT_TEMPLATE.format(
                current_time=context.current_time,
                market_status=context.market_status,
                holdings=holdings_text,
                signal_pool=signal_text,
                news_items=news_text
            )
            
            response = llm.chat([{"role": "user", "content": prompt}])
            content = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            json_match = None
            for line in content.split('\n'):
                if line.strip().startswith('['):
                    json_match = line
                    break
                if '{' in line and '}' in line:
                    start = content.find('[')
                    if start != -1:
                        json_match = content[start:]
                        break
            
            if not json_match:
                logger.warning("AI返回格式异常，回退到规则判断")
                return self._rule_judge(news_items, context)
            
            results = json.loads(json_match)
            
            push_items = [r for r in results if r.get("push", False)]
            if not push_items:
                return NewsJudgeResult(push=False, reason="AI判断无需推送")
            
            summary = self.summarize(push_items)
            return NewsJudgeResult(
                push=True,
                reason=f"AI判断: {len(push_items)}条需推送",
                summary=summary,
                priority="high"
            )
            
        except Exception as e:
            logger.warning(f"AI判断异常: {e}，回退到规则判断")
            return self._rule_judge(news_items, context)
    
    def _rule_judge(self, news_items: List[Dict], context: NewsContext) -> NewsJudgeResult:
        """规则判断（fallback）"""
        related_codes = set()
        for h in context.holdings:
            code = str(h.get("code", "")).strip()
            if code:
                related_codes.add(code.zfill(6))
        for s in context.signal_pool:
            code = str(s.get("code", "")).strip()
            if code:
                related_codes.add(code.zfill(6))
        
        push_items = []
        for item in news_items:
            title = str(item.get("title", "") or item.get("name", "")).lower()
            level = str(item.get("level", "normal"))
            category = str(item.get("category", "other"))
            
            is_related = any(code in title for code in related_codes)
            is_critical = level == "critical"
            is_important = level == "important"
            
            if context.market_status == "post_close":
                if is_critical or is_related:
                    push_items.append(item)
            else:
                if is_critical or (is_important and is_related):
                    push_items.append(item)
        
        if not push_items:
            return NewsJudgeResult(push=False, reason="规则判断无需推送")
        
        summary = self.summarize(push_items[:5])
        return NewsJudgeResult(
            push=True,
            reason=f"规则判断: {len(push_items)}条需推送",
            summary=summary,
            priority="medium"
        )
    
    def summarize(self, news_items: List[Dict]) -> str:
        """AI生成摘要"""
        llm = self._get_llm()
        if llm is None:
            return "\n".join([
                f"- {item.get('title', item.get('name', ''))[:50]}"
                for item in news_items[:5]
            ])
        
        try:
            news_text = "\n".join([
                f"- {item.get('title', item.get('name', ''))}"
                for item in news_items[:10]
            ])
            
            prompt = SUMMARIZE_PROMPT.format(news_items=news_text)
            response = llm.chat([{"role": "user", "content": prompt}])
            return response.content.strip() if hasattr(response, 'content') else str(response).strip()
        except Exception as e:
            logger.warning(f"AI摘要生成失败: {e}")
            return "\n".join([
                f"- {item.get('title', item.get('name', ''))[:50]}"
                for item in news_items[:5]
            ])


class NewsFilter:
    """资讯过滤器 - 去重"""
    
    def __init__(self, dedup_hours: int = None):
        if dedup_hours is None:
            dedup_hours = int(os.environ.get("NEWS_DEDUP_HOURS", "24"))
        self.dedup_hours = dedup_hours
        self._seen_ids: Dict[str, float] = {}
    
    def is_duplicate(self, news_id: str) -> bool:
        """检查是否重复"""
        now = time.time()
        if news_id in self._seen_ids:
            if now - self._seen_ids[news_id] < self.dedup_hours * 3600:
                return True
            else:
                del self._seen_ids[news_id]
        self._seen_ids[news_id] = now
        return False
    
    def mark_pushed(self, news_id: str) -> None:
        """标记已推送"""
        self._seen_ids[news_id] = time.time()
    
    def clear_expired(self) -> None:
        """清理过期ID"""
        now = time.time()
        expired = [
            id for id, ts in self._seen_ids.items()
            if now - ts >= self.dedup_hours * 3600
        ]
        for id in expired:
            del self._seen_ids[id]


_news_judge: Optional[AINewsJudge] = None
_news_filter: Optional[NewsFilter] = None


def get_news_judge() -> AINewsJudge:
    """获取AI资讯判断实例"""
    global _news_judge
    if _news_judge is None:
        _news_judge = AINewsJudge()
    return _news_judge


def get_news_filter() -> NewsFilter:
    """获取资讯过滤器实例"""
    global _news_filter
    if _news_filter is None:
        _news_filter = NewsFilter()
    return _news_filter
