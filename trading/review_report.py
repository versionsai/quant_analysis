# -*- coding: utf-8 -*-
"""
综合复盘报告生成模块
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def _format_review_lines(title: str, lines: List[str]) -> List[str]:
    """格式化复盘区块。"""
    block = [f"【{title}】"]
    block.extend([str(line).strip() for line in lines if str(line).strip()])
    if len(block) == 1:
        block.append("暂无数据")
    return block


def build_runtime_review_report(service) -> Dict:
    """基于看板服务构建真实数据库复盘报告。"""
    overview = service.get_overview()
    holdings = service.get_holdings()
    signal_pool = service.get_signal_pool_all(limit=20)
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
    signal_summary = signal_review.get("summary", {}) if isinstance(signal_review, dict) else {}
    signal_groups_review = signal_review.get("groups", []) if isinstance(signal_review, dict) else []
    timing_summary = timing_review.get("summary", {}) if isinstance(timing_review, dict) else {}
    timing_groups = timing_review.get("groups", []) if isinstance(timing_review, dict) else []
    latest_signal = latest.get("signal_pool_any") or {}

    blocks = [
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
                f"信号池展示 {len(signal_pool.get('display_rows', []) if isinstance(signal_pool, dict) else [])} 条 | "
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
        _format_review_lines("最新信号池展示", [
            (
                f"{item.get('date', '')} {item.get('code', '')} {item.get('name', '')} | "
                f"{item.get('signal_type', '')} | 价格 {float(item.get('price', 0.0) or 0.0):.2f} | "
                f"{item.get('reason', '')}"
            )
            for item in (signal_pool.get("display_rows", []) if isinstance(signal_pool, dict) else [])[:6]
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
            f"当前持仓数: {summary.get('holding_count', 0)} | 当前展示信号数: {len(signal_pool.get('display_rows', []) if isinstance(signal_pool, dict) else [])}",
            "复盘结论: 优先围绕活跃信号、当前持仓与择时试验结论做次日计划。",
        ]),
    ]

    report_text = "\n\n".join("\n".join(block) for block in blocks)
    return {
        "generated_at": overview.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "database_path": overview.get("database_path", ""),
        "overview": overview,
        "holdings": holdings,
        "signal_pool": signal_pool,
        "trade_points": trade_points,
        "signal_review": signal_review,
        "timing_review": timing_review,
        "timing_experiments": timing_experiments,
        "report_text": report_text,
    }


def build_runtime_review_report_from_db(db_path: str = "") -> Dict:
    """从数据库路径直接生成综合复盘报告。"""
    from dashboard import DashboardService

    service = DashboardService(db_path=db_path or None)
    return build_runtime_review_report(service)


def save_runtime_review_report(report: Dict, reports_dir: str = "./runtime/reports") -> Dict[str, str]:
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
