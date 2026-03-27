# -*- coding: utf-8 -*-
"""
A 股量化交易主程序
ETF/LOF + Price Action + MACD + Weak-to-Strong strategies
"""
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
import pandas as pd

load_dotenv()
load_dotenv(".env.local", override=True)
from data import DataSource, get_st_pool, get_dynamic_pool, get_pool_generator
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    TACOStrategy,
    TACOOilStrategy,
    WeakToStrongSelector,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
    TacoWeakStrongParams,
    TacoWeakStrongSelector,
    TacoWeakStrongTimingStrategy,
)
from backtest import BacktestEngine, PerformanceAnalyzer, SelectorBacktestEngine
from trading.review_report import build_runtime_review_report, save_runtime_review_report
from agents import (
    rank_experiment_candidates,
    save_best_config_snapshot,
    StrategyTuningAdvisor,
    run_strategy_experiments,
    save_experiment_report,
    save_tuning_review,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def build_weak_strong_backtest_params() -> WeakToStrongParams:
    """构建更宽松的弱转强回测参数。"""
    return WeakToStrongParams(
        limit_up_window=20,
        shrink_ratio=0.75,
        pullback_days=10,
        volume_multiple=1.35,
        min_rally_pct=1.2,
        total_window=40,
        breakdown_drop_pct=-5.5,
        breakdown_volume_multiple=2.2,
        breakdown_lookback=3,
        max_pullback_pct=18.0,
        require_confirm_open_above_prev_close=False,
        max_confirm_gap_pct=9.0,
        min_close_position_ratio=0.45,
        prior_weak_upper_shadow_ratio=0.55,
        prior_weak_volume_multiple=1.6,
    )


def build_taco_weak_strong_params() -> TacoWeakStrongParams:
    """构建 TACO + 弱转强组合参数。"""
    return TacoWeakStrongParams(
        weak_params=build_weak_strong_backtest_params(),
        taco_variant="taco",
        taco_candidate_threshold=0.16,
        taco_buy_score_threshold=0.36,
        combined_score_threshold=0.44,
        selector_stage_floor=3,
        selector_score_floor=26.0,
        stage4_buy_score_floor=0.50,
        stage3_buy_score_floor=0.56,
    )


def get_weak_strong_proxy_symbols(
    data_source: DataSource,
    start_date: str,
    limit: int = 20,
) -> list:
    """构建弱转强历史热点代理池。"""
    candidate_universe = [
        "000625", "000880", "000938", "000977", "000983",
        "002085", "002156", "002230", "002281", "002384",
        "002460", "002466", "002555", "002594", "002709",
        "002837", "002896", "002920", "003005", "300024",
        "300059", "300274", "300308", "300442", "300502",
        "300624", "300750", "300803", "300857", "300999",
        "301078", "301236", "600418", "600580", "600602",
        "600733", "600895", "601127", "603019", "603283",
        "603296", "603308", "603369", "603596", "603686",
        "603687", "688008", "688041", "688111", "688169",
    ]
    try:
        start_dt = datetime.strptime(start_date.replace("-", ""), "%Y%m%d")
    except Exception:
        start_dt = datetime.now()
    hist_start = (start_dt - timedelta(days=120)).strftime("%Y%m%d")
    hist_end = (start_dt - timedelta(days=1)).strftime("%Y%m%d")

    scored_rows = []
    for symbol in candidate_universe:
        try:
            df = data_source.get_kline(symbol, hist_start, hist_end)
            if df is None or df.empty or len(df) < 20:
                continue
            work_df = df.tail(60).copy()
            close = pd.to_numeric(work_df.get("close"), errors="coerce")
            volume = pd.to_numeric(work_df.get("volume"), errors="coerce")
            if close is None or close.empty or volume is None or volume.empty:
                continue
            latest_close = float(close.iloc[-1] or 0.0)
            if latest_close <= 0:
                continue
            base_idx = max(0, len(close) - 20)
            base_close = float(close.iloc[base_idx] or latest_close)
            ret_20 = (latest_close / base_close - 1.0) if base_close > 0 else 0.0
            max_day_gain = float(close.pct_change().tail(20).max() or 0.0)
            volatility = float(close.pct_change().tail(20).std() or 0.0)
            vol_ma20 = float(volume.tail(20).mean() or 0.0)
            vol_ratio = float((volume.tail(5).mean() / vol_ma20) if vol_ma20 > 0 else 1.0)
            hot_score = ret_20 * 45.0 + max_day_gain * 35.0 + volatility * 25.0 + min(vol_ratio, 3.0) * 8.0
            scored_rows.append((hot_score, symbol))
        except Exception:
            continue

    scored_rows.sort(reverse=True)
    symbols = [symbol for _, symbol in scored_rows[:limit]]
    if symbols:
        return symbols
    return candidate_universe[:limit]


def get_taco_fund_candidates(data_source: DataSource, timeout_sec: float = 15.0) -> list:
    """获取 TACO 默认基金池，超时则回退到内置 ETF/LOF 池。"""
    try:
        pool = get_dynamic_pool(pool_type="etf_lof", limit=30, db_path=os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"))
        rows = pool.get_t0_products_first()[:30]
        if rows:
            return rows
    except Exception as e:
        logger.error(f"TACO ETF/LOF dynamic pool load failed: {e}")
        return []


def get_backtest_symbols(strategy_name: str, data_source: DataSource, start_date: str = "20250101") -> list:
    """根据策略返回默认回测标的"""
    if strategy_name in {"taco", "taco_oil"}:
        products = get_taco_fund_candidates(data_source, timeout_sec=15.0)[:20]
        symbols = [str(item.get("code", "")).zfill(6) for item in products if item.get("code")]
        if symbols:
            return symbols
    if strategy_name in {"weak_strong", "taco_weak_strong"}:
        merged_symbols = []
        try:
            pool = get_dynamic_pool(
                pool_type="stock",
                limit=30,
                db_path=os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"),
            )
            symbols = [str(item.get("code", "")).zfill(6) for item in pool.get_t0_products_first() if item.get("code")]
            if symbols:
                merged_symbols.extend(symbols[:20])
        except Exception as e:
            logger.warning(f"WeakStrong dynamic pool load failed, fallback to hot-stock pool: {e}")

        try:
            generator = get_pool_generator(os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"))
            hot_products = generator.generate_hot_stock_pool(max_stocks=20)
            symbols = [str(item.code).zfill(6) for item in hot_products if getattr(item, "code", "")]
            if symbols:
                merged_symbols.extend(symbols[:20])
        except Exception as e:
            logger.warning(f"WeakStrong hot-stock pool load failed, fallback to preset symbols: {e}")

        merged_symbols.extend(get_weak_strong_proxy_symbols(data_source, start_date=start_date, limit=20))
        normalized = []
        seen = set()
        for symbol in merged_symbols:
            code = str(symbol).zfill(6)
            if code and code not in seen:
                seen.add(code)
                normalized.append(code)
        return normalized[:20]
    return [
        "600000", "600036", "600519", "601318", "600887",
        "000001", "000002", "000858", "000333", "000651",
        "300750", "300059", "300015", "002594", "002415",
        "601012", "601166", "600030", "600900", "600028",
    ]


def build_strategy(strategy_name: str):
    """根据策略名称构建策略实例"""
    if strategy_name == "macd":
        return MACDStrategy(fast=12, slow=26, signal=9)
    if strategy_name == "pa":
        return PriceActionStrategy(lookback=20)
    if strategy_name == "breakout":
        return BreakoutStrategy(lookback=20)
    if strategy_name == "weak_strong":
        return WeakToStrongTimingStrategy(params=build_weak_strong_backtest_params())
    if strategy_name == "taco":
        return TACOStrategy()
    if strategy_name == "taco_oil":
        return TACOOilStrategy()
    if strategy_name == "taco_weak_strong":
        return TacoWeakStrongTimingStrategy(params=build_taco_weak_strong_params())
    return PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )


def get_strategy_display_name(strategy_name: str) -> str:
    """获取策略展示名称"""
    mapping = {
        "pa_macd": "PriceAction + MACD",
        "macd": "MACD",
        "pa": "PriceAction",
        "breakout": "Breakout",
        "taco": "TACO Event Recovery",
        "taco_oil": "TACO-OIL Strategy",
        "taco_weak_strong": "TACO + WeakStrong",
    }
    return mapping.get(strategy_name, strategy_name)


def get_etf_lof_pool():
    """获取 ETF/LOF 股票池"""
    print("=" * 50)
    print("获取 ETF/LOF 股票池...")
    print("=" * 50)
    
    import os
    pool = get_st_pool("etf_lof", DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")))
    
    print(f"\n共获取 {len(pool)} 只 ETF/LOF 产品")
    
    t0_products = [p for p in pool.get_t0_products_first() if p.get("t0", False)]
    print(f"T+0 products: {len(t0_products)}")
    
    print("\n成交额 TOP 10 产品:")
    top_products = sorted(pool.get_t0_products_first(), key=lambda x: -x.get("amount", 0))[:10]
    for i, p in enumerate(top_products, 1):
        t0_flag = " T+0" if p.get("t0") else ""
        print(f"  {i}. {p.get('code')} {p.get('name')} - 成交额 {p.get('amount', 0)/1e8:.2f} 亿{t0_flag}")
    
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
    
    print("\nETF/LOF 池（T+0 优先）:")
    for i, p in enumerate(result["etf_lof"][:10], 1):
        t0_flag = " T+0" if p.t0 else ""
        print(f"  {i}. {p.code} {p.name} - 成交额 {p.amount/1e8:.2f} 亿 评分: {p.score:.0f}{t0_flag}")
    
    print("\n热点股票池（中高风险优先）:")
    for i, p in enumerate(result["stock"][:10], 1):
        risk_emoji = {"high": "[HIGH]", "medium_high": "[MEDIUM_HIGH]", "medium": "[MEDIUM]"}.get(p.risk_level, "[INFO]")
        print(f"  {i}. {p.code} {p.name} - 涨幅: {p.change_pct:+.2f}% 风险: {p.risk_level} 评分: {p.score:.0f} {risk_emoji} {p.reason}")
    
    summary = generator.get_pool_summary()
    print(f"\n股票池摘要: 总计 {summary['total']} 只, 已更新 {summary['updated']}")
    for ptype, cnt in summary["by_type"].items():
        print(f"  {ptype}: {cnt}")
    
    return result


def run_weak_strong_scan():
    """运行弱转强选股扫描"""
    import os
    print("=" * 60)
    print("弱转强选股扫描（双策略：PA+MACD + Weak-to-Strong）")
    print("=" * 60)
    
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    
    from trading import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=5, stock_count=10, db_path=db_path)
    
    results = monitor.scan_market()
    
    print("\nETF 推荐:")
    for r in results["etf"]:
        print(f"  {r.code} {r.name} - {r.signal_type} @ {r.price:.4f} ({r.reason})")
    
    print("\nA 股推荐（PA+MACD + Weak-to-Strong）:")
    dual_signals = [s for s in results["stock"] if s.dual_signal]
    print(f"  双重信号（{len(dual_signals)} 只）:")
    for s in dual_signals[:5]:
        print(f"    * {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} ({s.reason})")
    
    print(f"\n  单信号（{len(results['stock']) - len(dual_signals)} 只）:")
    for s in results["stock"]:
        if not s.dual_signal:
            ws_tag = f"[WS {s.ws_stage}/4]" if s.ws_stage > 0 else ""
            print(f"    {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} {ws_tag}({s.reason})")
    
    return results


def run_backtest_with_trades(strategy_name: str = "taco"):
    """运行回测并展示详细交易记录"""
    print("\n" + "=" * 60)
    print("A 股量化选股 + 择时回测")
    print("=" * 60)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    # 使用 A 股主板/创业板股票
    pool_symbols = get_backtest_symbols(strategy_name, data_source, start_date="20250101")
    
    code_to_name = {
        "600000": "浦发银行", "600036": "招商银行", "600519": "贵州茅台",
        "601318": "中国平安", "600887": "伊利股份", "000001": "平安银行",
        "000002": "万科A", "000858": "五粮液", "000333": "美的集团",
        "000651": "格力电器", "300750": "宁德时代", "300059": "东方财富",
        "300015": "爱尔眼科", "002594": "比亚迪", "002415": "海康威视",
        "601012": "隆基绿能", "601166": "兴业银行", "600030": "中信证券",
        "600900": "长江电力", "600028": "中国石化",
    }
    
    print(f"\n股票池: {len(pool_symbols)} 只")
    print(f"标的: {', '.join(pool_symbols[:5])}...")
    
    print(f"\n策略: {get_strategy_display_name(strategy_name)}")
    print("回测区间: 20250101 ~ 20260318")
    print(f"Initial capital: 1000000")

    if strategy_name in {"weak_strong", "taco_weak_strong"}:
        if strategy_name == "taco_weak_strong":
            combo_params = build_taco_weak_strong_params()
            selector = TacoWeakStrongSelector(combo_params)
            timing_strategy = TacoWeakStrongTimingStrategy(params=combo_params)
        else:
            ws_params = build_weak_strong_backtest_params()
            selector = WeakToStrongSelector(ws_params)
            timing_strategy = WeakToStrongTimingStrategy(params=ws_params)
        engine = SelectorBacktestEngine(
            selector=selector,
            timing_strategy=timing_strategy,
            initial_capital=1000000,
        )
        result = engine.run(
            pool_symbols=pool_symbols,
            start_date="20250101",
            end_date="20260318",
            data_source=data_source,
            select_top_n=min(8, len(pool_symbols)),
            rebalance_freq=15,
        )
    else:
        strategy = build_strategy(strategy_name)
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
    if result.benchmark_metrics:
        print("\n基准对比:")
        for item in result.benchmark_metrics.values():
            print(
                f"  {item['name']}: 收益 {item['total_return']:.2%} | "
                f"超额 {item['excess_return']:+.2%} | 回撤 {item['max_drawdown']:.2%}"
            )
    if result.phase_metrics:
        print("\n分阶段验证:")
        for item in result.phase_metrics:
            print(
                f"  {item['phase']} {item['start']}~{item['end']} "
                f"({item['days']}天): 策略 {item['total_return']:.2%} | "
                f"基准 {item['benchmark_return']:.2%} | 超额 {item['excess_return']:+.2%}"
            )
    if result.signal_summary:
        print("\n信号摘要:")
        print(
            f"  候选 {int(result.signal_summary.get('candidate_count', 0))} | "
            f"放行 {int(result.signal_summary.get('gated_count', 0))} | "
            f"放行率 {result.signal_summary.get('gate_pass_rate', 0.0):.2%} | "
            f"平均候选分 {result.signal_summary.get('avg_candidate_score', 0.0):.2f}"
        )
    
    print("\n" + "=" * 60)
    print("详细交易记录")
    print("=" * 60)
    print("Date         Code       Name            Action Price      Qty        Fee")
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
    print("持仓情况（期末）")
    print("=" * 60)
    final_positions = [(sym, pos) for sym, pos in engine.portfolio.positions.items()]
    if final_positions:
        print("Code       Name            Qty        Cost       Current    PnL")
        print("-" * 70)
        for sym, pos in final_positions:
            name = code_to_name.get(sym, "")[:12]
            pnl = (pos.current_price - pos.cost) / pos.cost * 100
            print(f"{sym:<10} {name:<15} {pos.quantity:<10} {pos.cost:<10.4f} {pos.current_price:<10.4f} {pnl:+.2f}%")
    else:
        print("  无持仓")
    
    return result


def run_taco_priority_scan(strategy_name: str = "taco"):
    """运行 TACO ETF/LOF 优先扫描"""
    print("\n" + "=" * 60)
    print("TACO ETF/LOF Priority Scan")
    print("=" * 60)

    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    strategy = build_strategy(strategy_name)
    products = get_taco_fund_candidates(data_source, timeout_sec=15.0)
    signals = []

    for item in products:
        symbol = str(item.get("code", "")).zfill(6)
        name = str(item.get("name", "") or "")
        if not symbol:
            continue
        df = data_source.get_kline(symbol, "20250101", "20260331")
        if df is None or df.empty:
            continue
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        signal = strategy.on_bar(symbol, df)
        if signal and (signal.signal != 0 or float(signal.candidate_score or 0.0) > 0):
            signals.append(
                {
                    "code": symbol,
                    "name": name,
                    "signal": "BUY" if signal.signal > 0 else "SELL",
                    "weight": float(signal.weight or 0.0),
                    "candidate_score": float(signal.candidate_score or 0.0),
                    "gate_passed": bool(signal.gate_passed),
                    "gate_reason": str(signal.gate_reason or ""),
                }
            )

    signals = sorted(signals, key=lambda item: (-item["candidate_score"], -item["weight"], item["code"]))
    print(f"strategy: {get_strategy_display_name(strategy_name)}")
    print(f"etf/lof candidates: {len(products)}")
    print(f"signals: {len(signals)}")
    for row in signals[:10]:
        gate_tag = "PASS" if row["gate_passed"] else "BLOCK"
        print(
            f"{row['code']} {row['name']} {row['signal']} "
            f"candidate={row['candidate_score']:.2f} weight={row['weight']:.2f} {gate_tag}"
        )
    return signals


def run_strategy_comparison():
    """策略对比"""
    print("\n" + "=" * 50)
    print("策略对比测试")
    print("=" * 50)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    stock_symbols = get_backtest_symbols("taco", data_source, start_date="20250101")
    weak_strong_symbols = get_backtest_symbols("weak_strong", data_source, start_date="20250101")
    taco_weak_strong_symbols = get_backtest_symbols("taco_weak_strong", data_source, start_date="20250101")
    taco_symbols = get_backtest_symbols("taco", data_source, start_date="20250101")

    strategies = {
        "PriceAction+MACD": ("single", PriceActionMACDStrategy(lookback=20), stock_symbols),
        "MACD": ("single", MACDStrategy(fast=12, slow=26, signal=9), stock_symbols),
        "PriceAction": ("single", PriceActionStrategy(lookback=20), stock_symbols),
        "Breakout": ("single", BreakoutStrategy(lookback=20), stock_symbols),
        "WeakStrong": (
            "selector",
            (
                WeakToStrongSelector(build_weak_strong_backtest_params()),
                WeakToStrongTimingStrategy(params=build_weak_strong_backtest_params()),
            ),
            weak_strong_symbols,
        ),
        "TACO+WeakStrong": (
            "selector",
            (
                TacoWeakStrongSelector(build_taco_weak_strong_params()),
                TacoWeakStrongTimingStrategy(params=build_taco_weak_strong_params()),
            ),
            taco_weak_strong_symbols,
        ),
        "TACO": ("single", TACOStrategy(), taco_symbols),
        "TACO-OIL": ("single", TACOOilStrategy(), taco_symbols),
    }
    
    results = {}
    
    for name, (engine_type, strategy_or_pair, symbols) in strategies.items():
        print(f"\n测试策略: {name}")

        if engine_type == "selector":
            selector, timing_strategy = strategy_or_pair
            engine = SelectorBacktestEngine(
                selector=selector,
                timing_strategy=timing_strategy,
                initial_capital=1000000,
            )
            result = engine.run(
                pool_symbols=symbols,
                start_date="20240101",
                end_date="20240630",
                data_source=data_source,
                select_top_n=min(8, len(symbols)),
                rebalance_freq=15,
            )
        else:
            engine = BacktestEngine(strategy=strategy_or_pair, initial_capital=1000000)
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
    parser.add_argument("--once", action="store_true", help="run once only")
    parser.add_argument("--schedule", action="store_true", help="start scheduler")
    parser.add_argument("--bark-key", type=str, default="", help="Bark key")
    
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
    from strategy.analysis.emotion import build_emotion_market_context
    
    analyzer = MultiDimensionalAnalyzer()
    market_context = build_emotion_market_context(trade_date=datetime.now().strftime("%Y%m%d"))
    
    market_emotion = analyzer.market_analyzer.get_market_emotion()
    if market_emotion:
        print("\n" + market_emotion.summary())
    
    sector_emotion = analyzer.sector_analyzer.analyze_sectors()
    if sector_emotion.success:
        print(f"\n板块情绪评分: {sector_emotion.score:.1f}")
        hot = sector_emotion.raw_data.get("hot_sectors", [])
        if hot:
            print(f"热门板块: {', '.join(hot[:5])}")

    print("\n增强情绪上下文:")
    print(
        f"周期={market_context.get('market_cycle', '')} | "
        f"空间={market_context.get('space_score', 0.0):.2f}({market_context.get('space_level', '')}) | "
        f"过热={market_context.get('overheat', 0.0):.2f}({market_context.get('overheat_risk', '')}) | "
        f"建议仓位={market_context.get('recommended_exposure', 0.0):.2f}"
    )
    reasons = list(market_context.get("reasons", []) or [])
    if reasons:
        print(f"增强理由: {'; '.join(reasons[:4])}")
    
    return {"market": market_emotion, "sector": sector_emotion, "context": market_context}


def run_review():
    """运行每日复盘"""
    print("=" * 60)
    print("每日复盘")
    print("=" * 60)
    from dashboard import DashboardService

    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    service = DashboardService(db_path=db_path)
    report = build_runtime_review_report(service)
    saved = save_runtime_review_report(report)

    print("\n" + str(report.get("report_text", "")))
    print("\n" + "=" * 60)
    print("报告已生成")
    print(f"Markdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")

    return {
        "report": report,
        "saved": saved,
    }


def run_tuning_review(strategy_name: str = "taco"):
    """运行回测并生成调优建议"""
    result = run_backtest_with_trades(strategy_name)
    advisor = StrategyTuningAdvisor()
    review = advisor.review_backtest(strategy_name, result)
    saved = save_tuning_review(review)

    print("\n" + "=" * 60)
    print("策略调优建议")
    print("=" * 60)
    print(str(review.get("summary", "") or ""))
    for index, item in enumerate(review.get("suggestions", []) or [], 1):
        print(f"{index}. [{item.get('priority', '')}] {item.get('target', '')}: {item.get('reason', '')}")
    print(f"\nMarkdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")
    return {"result": result, "review": review, "saved": saved}


def run_tuning_experiments(strategy_name: str = "taco"):
    """运行调优建议并自动批量实验"""
    result = run_backtest_with_trades(strategy_name)
    advisor = StrategyTuningAdvisor()
    review = advisor.review_backtest(strategy_name, result)
    experiments = list(review.get("experiments", []) or [])
    if not experiments:
        print("\n暂无可运行的实验建议。")
        return {"result": result, "review": review, "experiments": []}

    rows = run_strategy_experiments(strategy_name, experiments)
    recommended = rank_experiment_candidates(rows)
    saved = save_experiment_report(strategy_name, review, rows, recommended_rows=recommended)
    snapshot = save_best_config_snapshot(strategy_name, recommended)

    print("\n" + "=" * 60)
    print("批量实验结果")
    print("=" * 60)
    for index, item in enumerate(rows, 1):
        print(
            f"{index}. {item['name']} | 收益 {item['total_return']:.2%} | "
            f"夏普 {item['sharpe_ratio']:.2f} | 回撤 {item['max_drawdown']:.2%} | 交易 {item['trade_count']}"
        )
    if recommended:
        print("\n推荐候选:")
        for index, item in enumerate(recommended, 1):
            print(
                f"{index}. {item['name']} | 深证成指超额 {item.get('primary_excess_return', 0.0):+.2%} | "
                f"收益 {item['total_return']:.2%} | 夏普 {item['sharpe_ratio']:.2f} | "
                f"回撤 {item['max_drawdown']:.2%} | 排名分 {item.get('ranking_score', 0.0):.2f}"
            )
    if snapshot.get("json_path"):
        print(f"\nBest Config: {snapshot['json_path']}")
    print(f"\nMarkdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")
    return {
        "result": result,
        "review": review,
        "experiments": rows,
        "recommended": recommended,
        "saved": saved,
        "snapshot": snapshot,
    }


def run_params_mode(args):
    """参数管理模式"""
    from agents.tools.dynamic_params import get_current_params, get_param, set_param, get_param_history, reset_params_to_default
    
    if args.param_key and args.param_value is not None:
        result = set_param.invoke({
            "key": args.param_key,
            "value": args.param_value,
            "reason": args.reason or "手动调整",
            "source": "manual"
        })
        print(f"参数设置结果: {result}")
    elif args.param_key:
        value = get_param.invoke({"key": args.param_key})
        history = get_param_history.invoke({"key": args.param_key})
        print(f"参数 {args.param_key} 当前值: {value}")
        print(f"历史变更: {history}")
    else:
        params = get_current_params.invoke({})
        print("当前动态参数:")
        for k, v in params.items():
            print(f"  {k}: {v}")


def run_override_mode(args):
    """人工干预模式"""
    from data.recommend_db import ManualOverrideDB
    
    if not args.signal_id or not args.action:
        print("错误: --signal-id 和 --action 是必需参数")
        print("用法: python main.py --mode override --signal-id <id> --action <buy|sell|hold> --reason '<原因>'")
        return
    
    override_db = ManualOverrideDB()
    result = override_db.add_override(
        signal_id=args.signal_id,
        original_action="ai_decision",
        override_action=args.action,
        override_reason=args.reason or "人工干预",
        operator="human"
    )
    print(f"人工干预已记录: signal_id={args.signal_id}, action={args.action}")
    print(f"记录ID: {result}")


def run_optimize_mode(args):
    """优化模式"""
    from agents.multi_agent.optimizer_agent import get_optimizer
    
    optimizer = get_optimizer()
    print("开始每日优化...")
    
    summary = optimizer.get_daily_summary()
    print(f"\n当日摘要: {summary}")
    
    performance = optimizer.get_performance_analysis(lookback_days=30)
    print(f"\n信号来源表现: {performance.get('by_source', {})}")
    print(f"Agent表现: {performance.get('by_agent', {})}")
    
    result = optimizer.run_daily_optimization()
    print(f"\n优化结果:")
    print(f"  建议变更: {result.get('suggestions', [])}")
    print(f"  已应用: {result.get('applied_changes', [])}")
    print(f"  稳定性分数: {result.get('stability_score')}")


def run_debate_mode(args):
    """辩论模式"""
    from agents.multi_agent.debate_orchestrator import get_orchestrator
    
    if not args.symbols:
        print("用法: python main.py --mode debate --symbols 600036 000001")
        return
    
    orchestrator = get_orchestrator()
    
    from trading.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=1, stock_count=3)
    results = monitor.scan_market()
    
    signals = results.get("stock", [])[:3]
    
    for signal in signals:
        if not args.symbols or signal.code in args.symbols:
            print(f"\n{'='*50}")
            print(f"辩论分析: {signal.code} {signal.name}")
            print(f"{'='*50}")
            
            result = orchestrator.run_quick(
                signal={
                    "code": signal.code,
                    "name": signal.name,
                    "signal_type": signal.signal_type,
                    "score": signal.score,
                    "ws_stage": getattr(signal, "ws_stage", 0),
                    "ws_score": getattr(signal, "ws_score", 0),
                    "price": signal.price,
                    "change_pct": signal.change_pct,
                    "market_emotion_score": getattr(signal, "market_emotion_score", 50),
                    "dual_signal": signal.dual_signal,
                    "concept_strength_score": getattr(signal, "concept_strength_score", 0),
                },
                position_count=2,
            )
            
            print(f"乐观评分: {result.get('optimist_score', 0):.2f}")
            print(f"悲观评分: {result.get('pessimist_score', 0):.2f}")
            print(f"风控通过: {result.get('risk_passed', True)}")
            print(f"最终决策: {result.get('final_decision')}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare", "realtime", "pool-update", "weak-strong", "emotion-scan", "review", "taco-compare", "taco-monitor", "tune-review", "tune-experiments", "params", "override", "optimize", "debate"],
                       default="backtest", help="run mode")
    parser.add_argument("--symbols", nargs="+", help="stock symbols")
    parser.add_argument("--strategy", default="taco", 
                       choices=["pa_macd", "macd", "pa", "breakout", "weak_strong", "taco", "taco_oil", "taco_weak_strong"],
                       help="strategy name")
    parser.add_argument("--once", action="store_true", help="实时模式: 只运行一次")
    parser.add_argument("--schedule", action="store_true", help="实时模式: 启动定时调度")
    parser.add_argument("--bark-key", type=str, default="", help="Bark key")
    parser.add_argument("--signal-id", type=str, help="信号ID (用于override)")
    parser.add_argument("--action", type=str, choices=["buy", "sell", "hold"], help="操作 (用于override)")
    parser.add_argument("--reason", type=str, default="", help="原因")
    parser.add_argument("--param-key", type=str, help="参数名 (用于params模式)")
    parser.add_argument("--param-value", type=float, help="参数值 (用于params模式)")
    
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
    elif args.mode == "tune-review":
        run_tuning_review(args.strategy)
    elif args.mode == "tune-experiments":
        run_tuning_experiments(args.strategy)
    elif args.mode == "taco-compare":
        from taco_compare import run_taco_compare
        run_taco_compare()
    elif args.mode == "taco-monitor":
        run_taco_priority_scan(args.strategy if args.strategy in {"taco", "taco_oil"} else "taco")
    elif args.mode == "params":
        run_params_mode(args)
    elif args.mode == "override":
        run_override_mode(args)
    elif args.mode == "optimize":
        run_optimize_mode(args)
    elif args.mode == "debate":
        run_debate_mode(args)
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
        run_backtest_with_trades(args.strategy)


def run_params_mode(args):
    """参数管理模式"""
    from agents.tools.dynamic_params import get_current_params, get_param, set_param, get_param_history, reset_params_to_default
    
    if args.param_key and args.param_value is not None:
        result = set_param.invoke({
            "key": args.param_key,
            "value": args.param_value,
            "reason": args.reason or "手动调整",
            "source": "manual"
        })
        print(f"参数设置结果: {result}")
    elif args.param_key:
        value = get_param.invoke({"key": args.param_key})
        history = get_param_history.invoke({"key": args.param_key})
        print(f"参数 {args.param_key} 当前值: {value}")
        print(f"历史变更: {history}")
    else:
        params = get_current_params.invoke({})
        print("当前动态参数:")
        for k, v in params.items():
            print(f"  {k}: {v}")


def run_override_mode(args):
    """人工干预模式"""
    from data.recommend_db import ManualOverrideDB
    
    if not args.signal_id or not args.action:
        print("错误: --signal-id 和 --action 是必需参数")
        print("用法: python main.py --mode override --signal-id <id> --action <buy|sell|hold> --reason '<原因>'")
        return
    
    override_db = ManualOverrideDB()
    result = override_db.add_override(
        signal_id=args.signal_id,
        original_action="ai_decision",
        override_action=args.action,
        override_reason=args.reason or "人工干预",
        operator="human"
    )
    print(f"人工干预已记录: signal_id={args.signal_id}, action={args.action}")
    print(f"记录ID: {result}")


def run_optimize_mode(args):
    """优化模式"""
    from agents.multi_agent.optimizer_agent import get_optimizer
    
    optimizer = get_optimizer()
    print("开始每日优化...")
    
    summary = optimizer.get_daily_summary()
    print(f"\n当日摘要: {summary}")
    
    performance = optimizer.get_performance_analysis(lookback_days=30)
    print(f"\n信号来源表现: {performance.get('by_source', {})}")
    print(f"Agent表现: {performance.get('by_agent', {})}")
    
    result = optimizer.run_daily_optimization()
    print(f"\n优化结果:")
    print(f"  建议变更: {result.get('suggestions', [])}")
    print(f"  已应用: {result.get('applied_changes', [])}")
    print(f"  稳定性分数: {result.get('stability_score')}")


def run_debate_mode(args):
    """辩论模式"""
    from agents.multi_agent.debate_orchestrator import get_orchestrator
    
    if not args.symbols:
        print("用法: python main.py --mode debate --symbols 600036 000001")
        return
    
    orchestrator = get_orchestrator()
    
    from trading.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=1, stock_count=3)
    results = monitor.scan_market()
    
    signals = results.get("stock", [])[:3]
    
    for signal in signals:
        if not args.symbols or signal.code in args.symbols:
            print(f"\n{'='*50}")
            print(f"辩论分析: {signal.code} {signal.name}")
            print(f"{'='*50}")
            
            result = orchestrator.run_quick(
                signal={
                    "code": signal.code,
                    "name": signal.name,
                    "signal_type": signal.signal_type,
                    "score": signal.score,
                    "ws_stage": getattr(signal, "ws_stage", 0),
                    "ws_score": getattr(signal, "ws_score", 0),
                    "price": signal.price,
                    "change_pct": signal.change_pct,
                    "market_emotion_score": getattr(signal, "market_emotion_score", 50),
                    "dual_signal": signal.dual_signal,
                    "concept_strength_score": getattr(signal, "concept_strength_score", 0),
                },
                position_count=2,
            )
            
            print(f"乐观评分: {result.get('optimist_score', 0):.2f}")
            print(f"悲观评分: {result.get('pessimist_score', 0):.2f}")
            print(f"风控通过: {result.get('risk_passed', True)}")
            print(f"最终决策: {result.get('final_decision')}")


if __name__ == "__main__":
    main()


def run_params_mode(args):
    """参数管理模式"""
    from agents.tools.dynamic_params import get_current_params, get_param, set_param, get_param_history, reset_params_to_default
    
    if args.param_key and args.param_value is not None:
        result = set_param.invoke({
            "key": args.param_key,
            "value": args.param_value,
            "reason": args.reason or "手动调整",
            "source": "manual"
        })
        print(f"参数设置结果: {result}")
    elif args.param_key:
        value = get_param.invoke({"key": args.param_key})
        history = get_param_history.invoke({"key": args.param_key})
        print(f"参数 {args.param_key} 当前值: {value}")
        print(f"历史变更: {history}")
    else:
        params = get_current_params.invoke({})
        print("当前动态参数:")
        for k, v in params.items():
            print(f"  {k}: {v}")


def run_override_mode(args):
    """人工干预模式"""
    from data.recommend_db import ManualOverrideDB
    
    if not args.signal_id or not args.action:
        print("错误: --signal-id 和 --action 是必需参数")
        print("用法: python main.py --mode override --signal-id <id> --action <buy|sell|hold> --reason '<原因>'")
        return
    
    override_db = ManualOverrideDB()
    result = override_db.add_override(
        signal_id=args.signal_id,
        original_action="ai_decision",
        override_action=args.action,
        override_reason=args.reason or "人工干预",
        operator="human"
    )
    print(f"人工干预已记录: signal_id={args.signal_id}, action={args.action}")
    print(f"记录ID: {result}")


def run_optimize_mode(args):
    """优化模式"""
    from agents.multi_agent.optimizer_agent import get_optimizer
    
    optimizer = get_optimizer()
    print("开始每日优化...")
    
    summary = optimizer.get_daily_summary()
    print(f"\n当日摘要: {summary}")
    
    performance = optimizer.get_performance_analysis(lookback_days=30)
    print(f"\n信号来源表现: {performance.get('by_source', {})}")
    print(f"Agent表现: {performance.get('by_agent', {})}")
    
    result = optimizer.run_daily_optimization()
    print(f"\n优化结果:")
    print(f"  建议变更: {result.get('suggestions', [])}")
    print(f"  已应用: {result.get('applied_changes', [])}")
    print(f"  稳定性分数: {result.get('stability_score')}")


def run_debate_mode(args):
    """辩论模式"""
    from agents.multi_agent.debate_orchestrator import get_orchestrator
    
    if not args.symbols:
        print("用法: python main.py --mode debate --symbols 600036 000001")
        return
    
    orchestrator = get_orchestrator()
    
    from trading.realtime_monitor import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=1, stock_count=3)
    results = monitor.scan_market()
    
    signals = results.get("stock", [])[:3]
    
    for signal in signals:
        if not args.symbols or signal.code in args.symbols:
            print(f"\n{'='*50}")
            print(f"辩论分析: {signal.code} {signal.name}")
            print(f"{'='*50}")
            
            result = orchestrator.run_quick(
                signal={
                    "code": signal.code,
                    "name": signal.name,
                    "signal_type": signal.signal_type,
                    "score": signal.score,
                    "ws_stage": getattr(signal, "ws_stage", 0),
                    "ws_score": getattr(signal, "ws_score", 0),
                    "price": signal.price,
                    "change_pct": signal.change_pct,
                    "market_emotion_score": getattr(signal, "market_emotion_score", 50),
                    "dual_signal": signal.dual_signal,
                    "concept_strength_score": getattr(signal, "concept_strength_score", 0),
                },
                position_count=2,
            )
            
            print(f"乐观评分: {result.get('optimist_score', 0):.2f}")
            print(f"悲观评分: {result.get('pessimist_score', 0):.2f}")
            print(f"风控通过: {result.get('risk_passed', True)}")
            print(f"最终决策: {result.get('final_decision')}")
