# -*- coding: utf-8 -*-
"""
A股量化交易主程序
ETF/LOF + Price Action + MACD + 弱转强策略
"""
import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.local", override=True)
from data import DataSource, get_st_pool, get_dynamic_pool, get_pool_generator
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    WeakToStrongSelector,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
)
from backtest import BacktestEngine, PerformanceAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


def _format_review_lines(title: str, lines: list) -> list:
    """格式化复盘区块。"""
    block = [f"【{title}】"]
    block.extend([str(line).strip() for line in lines if str(line).strip()])
    if len(block) == 1:
        block.append("暂无数据")
    return block


def _build_runtime_review_report(service) -> dict:
    """基于真实数据库构建综合复盘报告。"""
    overview = service.get_overview()
    holdings = service.get_holdings()
    signal_pool = service.get_signal_pool_all(limit=20)
    recommends = service.get_recent_recommends(limit=10)
    trade_points = service.get_trade_points(limit=20)
    signal_review = service.get_signal_review(limit=30)
    timing_review = service.get_timing_review(limit=30)
    timing_experiments = service.get_timing_experiments()

    summary = overview.get("summary", {})
    latest = overview.get("latest", {})
    runtime_settings = overview.get("runtime_settings", {})
    signal_groups = signal_pool.get("groups", {}) if isinstance(signal_pool, dict) else {}
    active_signals = list(signal_groups.get("active", []))
    inactive_signals = list(signal_groups.get("inactive", []))
    timing_conclusion = timing_experiments.get("conclusion", {}) if isinstance(timing_experiments, dict) else {}

    sections = []
    sections.extend(_format_review_lines("复盘总览", [
        f"生成时间: {overview.get('generated_at', '--')}",
        f"数据库: {overview.get('database_path', '--')}",
        f"市场模式: {runtime_settings.get('market_regime_label', '未知')} | {runtime_settings.get('market_regime_description', '')}",
        (
            f"持仓 {summary.get('holding_count', 0)} 只 | "
            f"活跃信号 {summary.get('signal_pool_active_count', 0)} 条 | "
            f"最近失效 {summary.get('signal_pool_inactive_count', 0)} 条 | "
            f"股票池 {summary.get('stock_pool_count', 0)} 只"
        ),
        (
            f"荐股 {summary.get('recommend_count', 0)} 条 | "
            f"交易事件 {summary.get('trade_event_count', 0)} 条 | "
            f"已卖出 {summary.get('sell_trade_count', 0)} 笔 | "
            f"历史胜率 {float(summary.get('win_rate', 0.0) or 0.0):.1f}%"
        ),
        f"累计已实现收益: {float(summary.get('total_pnl', 0.0) or 0.0):+.2f}",
    ]))

    sections.extend(_format_review_lines("当前持仓", [
        (
            f"{item.get('code', '')} {item.get('name', '')} | "
            f"现价 {float(item.get('avg_current_price', 0.0) or 0.0):.2f} | "
            f"盈亏 {float(item.get('total_pnl_pct', 0.0) or 0.0):+.2f}% | "
            f"AI {item.get('ai_hint', '暂无')}"
        )
        for item in holdings[:8]
    ]))

    sections.extend(_format_review_lines("活跃信号池", [
        (
            f"{item.get('code', '')} {item.get('name', '')} | "
            f"{item.get('signal_type', '')} | {item.get('pool_type', '')} | "
            f"评分 {float(item.get('score', 0.0) or 0.0):.2f} | "
            f"{item.get('reason', '')}"
        )
        for item in active_signals[:8]
    ]))

    sections.extend(_format_review_lines("最近失效信号", [
        (
            f"{item.get('code', '')} {item.get('name', '')} | "
            f"{item.get('signal_type', '')} | {item.get('updated_label', item.get('updated_at', '--'))} | "
            f"{item.get('reason', '')}"
        )
        for item in inactive_signals[:5]
    ]))

    sections.extend(_format_review_lines("最近荐股", [
        (
            f"{item.get('date', '')} {item.get('code', '')} {item.get('name', '')} | "
            f"{item.get('signal_type', '')} | 价格 {float(item.get('price', 0.0) or 0.0):.2f} | "
            f"{item.get('reason', '')}"
        )
        for item in recommends[:6]
    ]))

    sections.extend(_format_review_lines("最近交易事件", [
        (
            f"{item.get('date', '')} {item.get('code', '')} {item.get('name', '')} | "
            f"{item.get('event_type', '')}/{item.get('signal_type', '')} | "
            f"价格 {float(item.get('price', 0.0) or 0.0):.2f} | {item.get('reason', '')}"
        )
        for item in trade_points[:8]
    ]))

    signal_summary = signal_review.get("summary", {}) if isinstance(signal_review, dict) else {}
    signal_groups_review = signal_review.get("groups", []) if isinstance(signal_review, dict) else []
    sections.extend(_format_review_lines("信号质量复盘", [
        (
            f"样本 {int(signal_summary.get('total_count', 0) or 0)} | "
            f"已完成 {int(signal_summary.get('closed_count', 0) or 0)} | "
            f"持有中 {int(signal_summary.get('open_count', 0) or 0)} | "
            f"胜率 {float(signal_summary.get('win_rate', 0.0) or 0.0):.1f}% | "
            f"平均收益率 {float(signal_summary.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
        ),
        *[
            (
                f"{group.get('group', '')}: 样本 {int(group.get('count', 0) or 0)} | "
                f"已完成 {int(group.get('closed_count', 0) or 0)} | "
                f"胜率 {float(group.get('win_rate', 0.0) or 0.0):.1f}% | "
                f"平均收益率 {float(group.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
            )
            for group in signal_groups_review[:4]
        ],
    ]))

    timing_summary = timing_review.get("summary", {}) if isinstance(timing_review, dict) else {}
    timing_groups = timing_review.get("groups", []) if isinstance(timing_review, dict) else []
    sections.extend(_format_review_lines("择时卖出复盘", [
        (
            f"样本 {int(timing_summary.get('total_count', 0) or 0)} | "
            f"胜率 {float(timing_summary.get('win_rate', 0.0) or 0.0):.1f}% | "
            f"平均收益率 {float(timing_summary.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
        ),
        *[
            (
                f"{group.get('reason', '')}: 次数 {int(group.get('count', 0) or 0)} | "
                f"胜率 {float(group.get('win_rate', 0.0) or 0.0):.1f}% | "
                f"平均收益率 {float(group.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
            )
            for group in timing_groups[:5]
        ],
    ]))

    sections.extend(_format_review_lines("择时参数试验", [
        timing_conclusion.get("title", ""),
        timing_conclusion.get("summary", ""),
        timing_conclusion.get("recommendation", ""),
    ]))

    latest_signal = latest.get("signal_pool_any") or {}
    sections.extend(_format_review_lines("今日结论", [
        f"当前最强信号: {latest_signal.get('code', '--')} {latest_signal.get('name', '--')} | {latest_signal.get('signal_type', '--')}",
        f"当前持仓数: {summary.get('holding_count', 0)} | 最新荐股数: {summary.get('recommend_count', 0)}",
        f"复盘结论: 优先围绕活跃信号、当前持仓与择时试验结论做次日计划。",
    ]))

    report_text = "\n\n".join("\n".join(block) for block in [
        _format_review_lines("复盘总览", [
            f"生成时间: {overview.get('generated_at', '--')}",
            f"数据库: {overview.get('database_path', '--')}",
            f"市场模式: {runtime_settings.get('market_regime_label', '未知')} | {runtime_settings.get('market_regime_description', '')}",
            (
                f"持仓 {summary.get('holding_count', 0)} 只 | "
                f"活跃信号 {summary.get('signal_pool_active_count', 0)} 条 | "
                f"最近失效 {summary.get('signal_pool_inactive_count', 0)} 条 | "
                f"股票池 {summary.get('stock_pool_count', 0)} 只"
            ),
            (
                f"荐股 {summary.get('recommend_count', 0)} 条 | "
                f"交易事件 {summary.get('trade_event_count', 0)} 条 | "
                f"已卖出 {summary.get('sell_trade_count', 0)} 笔 | "
                f"历史胜率 {float(summary.get('win_rate', 0.0) or 0.0):.1f}%"
            ),
            f"累计已实现收益: {float(summary.get('total_pnl', 0.0) or 0.0):+.2f}",
        ]),
        _format_review_lines("当前持仓", [
            (
                f"{item.get('code', '')} {item.get('name', '')} | "
                f"现价 {float(item.get('avg_current_price', 0.0) or 0.0):.2f} | "
                f"盈亏 {float(item.get('total_pnl_pct', 0.0) or 0.0):+.2f}% | "
                f"AI {item.get('ai_hint', '暂无')}"
            )
            for item in holdings[:8]
        ]),
        _format_review_lines("活跃信号池", [
            (
                f"{item.get('code', '')} {item.get('name', '')} | "
                f"{item.get('signal_type', '')} | {item.get('pool_type', '')} | "
                f"评分 {float(item.get('score', 0.0) or 0.0):.2f} | "
                f"{item.get('reason', '')}"
            )
            for item in active_signals[:8]
        ]),
        _format_review_lines("最近失效信号", [
            (
                f"{item.get('code', '')} {item.get('name', '')} | "
                f"{item.get('signal_type', '')} | {item.get('updated_label', item.get('updated_at', '--'))} | "
                f"{item.get('reason', '')}"
            )
            for item in inactive_signals[:5]
        ]),
        _format_review_lines("最近荐股", [
            (
                f"{item.get('date', '')} {item.get('code', '')} {item.get('name', '')} | "
                f"{item.get('signal_type', '')} | 价格 {float(item.get('price', 0.0) or 0.0):.2f} | "
                f"{item.get('reason', '')}"
            )
            for item in recommends[:6]
        ]),
        _format_review_lines("最近交易事件", [
            (
                f"{item.get('date', '')} {item.get('code', '')} {item.get('name', '')} | "
                f"{item.get('event_type', '')}/{item.get('signal_type', '')} | "
                f"价格 {float(item.get('price', 0.0) or 0.0):.2f} | {item.get('reason', '')}"
            )
            for item in trade_points[:8]
        ]),
        _format_review_lines("信号质量复盘", [
            (
                f"样本 {int(signal_summary.get('total_count', 0) or 0)} | "
                f"已完成 {int(signal_summary.get('closed_count', 0) or 0)} | "
                f"持有中 {int(signal_summary.get('open_count', 0) or 0)} | "
                f"胜率 {float(signal_summary.get('win_rate', 0.0) or 0.0):.1f}% | "
                f"平均收益率 {float(signal_summary.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
            ),
            *[
                (
                    f"{group.get('group', '')}: 样本 {int(group.get('count', 0) or 0)} | "
                    f"已完成 {int(group.get('closed_count', 0) or 0)} | "
                    f"胜率 {float(group.get('win_rate', 0.0) or 0.0):.1f}% | "
                    f"平均收益率 {float(group.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
                )
                for group in signal_groups_review[:4]
            ],
        ]),
        _format_review_lines("择时卖出复盘", [
            (
                f"样本 {int(timing_summary.get('total_count', 0) or 0)} | "
                f"胜率 {float(timing_summary.get('win_rate', 0.0) or 0.0):.1f}% | "
                f"平均收益率 {float(timing_summary.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
            ),
            *[
                (
                    f"{group.get('reason', '')}: 次数 {int(group.get('count', 0) or 0)} | "
                    f"胜率 {float(group.get('win_rate', 0.0) or 0.0):.1f}% | "
                    f"平均收益率 {float(group.get('avg_pnl_pct', 0.0) or 0.0):+.2f}%"
                )
                for group in timing_groups[:5]
            ],
        ]),
        _format_review_lines("择时参数试验", [
            timing_conclusion.get("title", ""),
            timing_conclusion.get("summary", ""),
            timing_conclusion.get("recommendation", ""),
        ]),
        _format_review_lines("今日结论", [
            f"当前最强信号: {latest_signal.get('code', '--')} {latest_signal.get('name', '--')} | {latest_signal.get('signal_type', '--')}",
            f"当前持仓数: {summary.get('holding_count', 0)} | 最新荐股数: {summary.get('recommend_count', 0)}",
            "复盘结论: 优先围绕活跃信号、当前持仓与择时试验结论做次日计划。",
        ]),
    ])

    return {
        "generated_at": overview.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "database_path": overview.get("database_path", ""),
        "overview": overview,
        "holdings": holdings,
        "signal_pool": signal_pool,
        "recommends": recommends,
        "trade_points": trade_points,
        "signal_review": signal_review,
        "timing_review": timing_review,
        "timing_experiments": timing_experiments,
        "report_text": report_text,
    }


def _save_runtime_review_report(report: dict, reports_dir: str = "./runtime/reports") -> dict:
    """保存综合复盘报告到文件。"""
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    generated_at = str(report.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    safe_date = generated_at[:10].replace("-", "")
    timestamp = generated_at[11:19].replace(":", "") if len(generated_at) >= 19 else datetime.now().strftime("%H%M%S")
    markdown_path = report_dir / f"review_{safe_date}_{timestamp}.md"
    json_path = report_dir / f"review_{safe_date}_{timestamp}.json"
    markdown_path.write_text(str(report.get("report_text", "")).strip() + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "markdown_path": str(markdown_path),
        "json_path": str(json_path),
    }


def get_etf_lof_pool():
    """获取ETF/LOF股票池"""
    print("=" * 50)
    print("获取ETF/LOF股票池...")
    print("=" * 50)
    
    import os
    pool = get_st_pool("etf_lof", DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")))
    
    print(f"\n总共获取到 {len(pool)} 只ETF/LOF产品")
    
    t0_products = [p for p in pool.get_t0_products_first() if p.get("t0", False)]
    print(f"其中 T+0 产品: {len(t0_products)} 只")
    
    print("\n成交额TOP 10产品:")
    top_products = sorted(pool.get_t0_products_first(), key=lambda x: -x.get("amount", 0))[:10]
    for i, p in enumerate(top_products, 1):
        t0_flag = "✓T+0" if p.get("t0") else ""
        print(f"  {i}. {p.get('code')} {p.get('name')} - 成交额: {p.get('amount', 0)/1e8:.2f}亿 {t0_flag}")
    
    return pool


def update_stock_pool():
    """更新每日股票池"""
    print("=" * 50)
    print("更新每日股票池...")
    print("=" * 50)
    
    import os
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    generator = get_pool_generator(db_path)
    
    result = generator.update_daily()
    
    print("\nETF/LOF 池 (T+0 优先):")
    for i, p in enumerate(result["etf_lof"][:10], 1):
        t0_flag = "✓T+0" if p.t0 else ""
        print(f"  {i}. {p.code} {p.name} - 成交额: {p.amount/1e8:.2f}亿 评分: {p.score:.0f} {t0_flag}")
    
    print(f"\n热点股票池 (中高风险优先):")
    for i, p in enumerate(result["stock"][:10], 1):
        risk_emoji = {"high": "🔴", "medium_high": "🟠", "medium": "🟡"}.get(p.risk_level, "⚪")
        print(f"  {i}. {p.code} {p.name} - 涨幅: {p.change_pct:+.2f}% 风险: {p.risk_level} 评分: {p.score:.0f} {risk_emoji} {p.reason}")
    
    summary = generator.get_pool_summary()
    print(f"\n股票池摘要: 总计 {summary['total']} 只, 更新于 {summary['updated']}")
    for ptype, cnt in summary["by_type"].items():
        print(f"  {ptype}: {cnt} 只")
    
    return result


def run_weak_strong_scan():
    """运行弱转强选股扫描"""
    import os
    print("=" * 60)
    print("弱转强选股扫描 (双策略: PA+MACD + 弱转强)")
    print("=" * 60)
    
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    
    from trading import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=5, stock_count=10, db_path=db_path)
    
    results = monitor.scan_market()
    
    print("\nETF推荐:")
    for r in results["etf"]:
        print(f"  {r.code} {r.name} - {r.signal_type} @ {r.price:.4f} ({r.reason})")
    
    print(f"\nA股推荐 (PA+MACD + 弱转强):")
    dual_signals = [s for s in results["stock"] if s.dual_signal]
    print(f"  双重信号({len(dual_signals)}只):")
    for s in dual_signals[:5]:
        print(f"    ⭐ {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} ({s.reason})")
    
    print(f"\n  单信号({len(results['stock']) - len(dual_signals)}只):")
    for s in results["stock"]:
        if not s.dual_signal:
            ws_tag = f"[弱转强{s.ws_stage}/4]" if s.ws_stage > 0 else ""
            print(f"    {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} {ws_tag}({s.reason})")
    
    return results


def run_backtest_with_trades():
    """运行回测并展示详细交易记录"""
    print("\n" + "=" * 60)
    print("A股量化选股+择时回测")
    print("=" * 60)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    # 使用A股主板/创业板股票
    pool_symbols = [
        "600000", "600036", "600519", "601318", "600887",
        "000001", "000002", "000858", "000333", "000651",
        "300750", "300059", "300015", "002594", "002415",
        "601012", "601166", "600030", "600900", "600028"
    ]
    
    code_to_name = {
        "600000": "浦发银行", "600036": "招商银行", "600519": "贵州茅台",
        "601318": "中国平安", "600887": "伊利股份", "000001": "平安银行",
        "000002": "万科A", "000858": "五粮液", "000333": "美的集团",
        "000651": "格力电器", "300750": "宁德时代", "300059": "东方财富",
        "300015": "爱尔眼科", "002594": "比亚迪", "002415": "海康威视",
        "601012": "隆基绿能", "601166": "兴业银行", "600030": "中信证券",
        "600900": "长江电力", "600028": "中国石化",
    }
    
    print(f"\n股票池: {len(pool_symbols)} 只 (A股主板/创业板)")
    print(f"标的: {', '.join(pool_symbols[:5])}...")
    
    strategy = PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )
    
    print("\n策略: PriceAction + MACD 复合策略")
    print(f"回测区间: 20250101 ~ 20260318")
    print(f"初始资金: 100万")
    
    engine = BacktestEngine(
        strategy=strategy,
        initial_capital=1000000,
    )
    
    result = engine.run(
        symbols=pool_symbols,
        start_date="20250101",
        end_date="20260318",
        data_source=data_source,
    )
    
    print("\n" + "=" * 60)
    print("回测结果")
    print("=" * 60)
    print(f"  总收益: {result.total_return:.2%}")
    print(f"  年化收益: {result.annual_return:.2%}")
    print(f"  夏普比率: {result.sharpe_ratio:.2f}")
    print(f"  最大回撤: {result.max_drawdown:.2%}")
    print(f"  胜率: {result.win_rate:.2%}")
    print(f"  交易次数: {len(result.trades)}")
    
    print("\n" + "=" * 60)
    print("详细交易记录")
    print("=" * 60)
    print(f"{'日期':<12} {'代码':<10} {'名称':<15} {'操作':<6} {'价格':<10} {'数量':<10} {'手续费'}")
    print("-" * 80)
    
    buy_count = 0
    sell_count = 0
    for trade in result.trades:
        name = code_to_name.get(trade.symbol, "")[:12]
        direction = "买入" if trade.direction == "buy" else "卖出"
        print(f"{trade.date.strftime('%Y-%m-%d'):<12} {trade.symbol:<10} {name:<15} {direction:<6} {trade.price:<10.4f} {trade.quantity:<10} {trade.commission:.2f}")
        if trade.direction == "buy":
            buy_count += 1
        else:
            sell_count += 1
    
    print(f"\n买入次数: {buy_count}, 卖出次数: {sell_count}")
    
    print("\n" + "=" * 60)
    print("持仓情况 (期末)")
    print("=" * 60)
    final_positions = [(sym, pos) for sym, pos in engine.portfolio.positions.items()]
    if final_positions:
        print(f"{'代码':<10} {'名称':<15} {'数量':<10} {'成本价':<10} {'当前价':<10} {'盈亏'}")
        print("-" * 70)
        for sym, pos in final_positions:
            name = code_to_name.get(sym, "")[:12]
            pnl = (pos.current_price - pos.cost) / pos.cost * 100
            print(f"{sym:<10} {name:<15} {pos.quantity:<10} {pos.cost:<10.4f} {pos.current_price:<10.4f} {pnl:+.2f}%")
    else:
        print("  无持仓")
    
    return result


def run_strategy_comparison():
    """策略对比"""
    print("\n" + "=" * 50)
    print("策略对比测试")
    print("=" * 50)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    pool = get_st_pool("etf_lof", data_source)
    products = pool.get_t0_products_first()[:15]
    symbols = [p["code"] for p in products]
    
    strategies = {
        "PriceAction+MACD": PriceActionMACDStrategy(lookback=20),
        "MACD": MACDStrategy(fast=12, slow=26, signal=9),
        "PriceAction": PriceActionStrategy(lookback=20),
        "Breakout": BreakoutStrategy(lookback=20),
    }
    
    results = {}
    
    for name, strategy in strategies.items():
        print(f"\n测试策略: {name}")
        
        engine = BacktestEngine(strategy=strategy, initial_capital=1000000)
        
        result = engine.run(
            symbols=symbols,
            start_date="20240101",
            end_date="20240630",
            data_source=data_source,
        )
        
        results[name] = result
        print(f"  总收益: {result.total_return:.2%}")
    
    print("\n" + "=" * 50)
    print("策略对比结果")
    print("=" * 50)
    for name, result in sorted(results.items(), key=lambda x: -x[1].total_return):
        print(f"  {name}: {result.total_return:.2%}")


def run_realtime():
    """运行实时选股推送"""
    from trading import run_realtime_scan, run_scheduler
    
    import argparse
    parser = argparse.ArgumentParser(description="实时选股推送")
    parser.add_argument("--once", action="store_true", help="只运行一次，不持续监控")
    parser.add_argument("--schedule", action="store_true", help="启动定时调度器")
    parser.add_argument("--bark-key", type=str, default="", help="Bark推送Key(可选；为空则不调用Bark)")
    
    args = parser.parse_args([])
    
    if args.bark_key:
        from trading import set_pusher_key
        set_pusher_key(args.bark_key)
    
    if args.schedule:
        print("启动定时调度器...")
        run_scheduler()
    else:
        print("运行单次实时扫描...")
        run_realtime_scan()


def run_emotion_scan():
    """运行情绪扫描"""
    print("=" * 60)
    print("市场情绪扫描")
    print("=" * 60)
    
    from strategy.analysis.multi_analyzer import MultiDimensionalAnalyzer
    
    analyzer = MultiDimensionalAnalyzer()
    
    market_emotion = analyzer.market_analyzer.get_market_emotion()
    if market_emotion:
        print("\n" + market_emotion.summary())
    
    sector_emotion = analyzer.sector_analyzer.analyze_sectors()
    if sector_emotion.success:
        print(f"\n板块情绪评分: {sector_emotion.score:.1f}")
        hot = sector_emotion.raw_data.get("hot_sectors", [])
        if hot:
            print(f"热门板块: {', '.join(hot[:5])}")
    
    return {"market": market_emotion, "sector": sector_emotion}


def run_review():
    """运行每日复盘"""
    print("=" * 60)
    print("每日复盘")
    print("=" * 60)
    from dashboard import DashboardService

    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    service = DashboardService(db_path=db_path)
    report = _build_runtime_review_report(service)
    saved = _save_runtime_review_report(report)

    print("\n" + str(report.get("report_text", "")))
    print("\n" + "=" * 60)
    print("报告已生成")
    print(f"Markdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")

    return {
        "report": report,
        "saved": saved,
    }


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare", "realtime", "pool-update", "weak-strong", "emotion-scan", "review"],
                       default="backtest", help="运行模式")
    parser.add_argument("--symbols", nargs="+", help="指定股票代码")
    parser.add_argument("--strategy", default="pa_macd", 
                       choices=["pa_macd", "macd", "pa", "breakout", "weak_strong"],
                       help="选择策略")
    parser.add_argument("--once", action="store_true", help="实时模式: 只运行一次")
    parser.add_argument("--schedule", action="store_true", help="实时模式: 启动定时调度器")
    parser.add_argument("--bark-key", type=str, default="", help="Bark推送Key(可选；为空则不调用Bark)")
    
    args = parser.parse_args()
    
    if args.mode == "pool":
        get_etf_lof_pool()
    elif args.mode == "pool-update":
        update_stock_pool()
    elif args.mode == "weak-strong":
        run_weak_strong_scan()
    elif args.mode == "compare":
        run_strategy_comparison()
    elif args.mode == "emotion-scan":
        run_emotion_scan()
    elif args.mode == "review":
        run_review()
    elif args.mode == "realtime":
        from trading import set_pusher_key
        if args.bark_key:
            set_pusher_key(args.bark_key)
        
        if args.schedule:
            print("启动定时调度器...")
            from trading import run_scheduler
            run_scheduler()
        else:
            print("运行实时选股扫描...")
            from trading import run_realtime_scan
            run_realtime_scan()
    else:
        run_backtest_with_trades()


if __name__ == "__main__":
    main()
