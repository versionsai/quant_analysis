# -*- coding: utf-8 -*-
"""
Top 预测模型

目标：
- 使用 sklearn 的 LogisticRegression 预测未来 1~3 天是否出现 >5% 回撤
- 保持训练、推理、持久化分离
- 方便被 backtest / realtime_monitor / dashboard 直接复用

特征定义：
features = [
    space_score,
    overheat,
    acc,
    zt_diff,
    eff_diff,
    leader_ret,
]

标签定义：
未来 1~3 天跌幅 > 5% -> 1，否则 0

示例数据结构：
[
    {
        "trade_date": "20260327",
        "symbol": "600580",
        "space_score": 0.72,
        "overheat": 0.63,
        "acc": 0.18,
        "zt_diff": 12,
        "eff_diff": 0.09,
        "leader_ret": 0.07,
        "label": 0
    }
]
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence
import os
import pickle

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TOP_MODEL_PATH = "./runtime/models/top_predict_model.pkl"

TOP_FEATURE_COLUMNS = [
    "space_score",
    "overheat",
    "acc",
    "zt_diff",
    "eff_diff",
    "leader_ret",
]


@dataclass
class TopFeatureRow:
    """单条 Top 预测特征。"""

    trade_date: str
    symbol: str
    space_score: float
    overheat: float
    acc: float
    zt_diff: float
    eff_diff: float
    leader_ret: float
    label: Optional[int] = None

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return asdict(self)


@dataclass
class TopPrediction:
    """单条 Top 风险预测结果。"""

    symbol: str
    trade_date: str
    top_prob: float
    decision: str
    threshold_buy: float = 0.4
    threshold_sell: float = 0.6

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "symbol": self.symbol,
            "trade_date": self.trade_date,
            "top_prob": round(float(self.top_prob or 0.0), 4),
            "decision": self.decision,
            "threshold_buy": round(float(self.threshold_buy or 0.0), 4),
            "threshold_sell": round(float(self.threshold_sell or 0.0), 4),
        }


class TopPredictModel:
    """Top 风险预测模型。"""

    def __init__(
        self,
        threshold_buy: float = 0.4,
        threshold_sell: float = 0.6,
        random_state: int = 42,
    ):
        self.threshold_buy = float(threshold_buy)
        self.threshold_sell = float(threshold_sell)
        self.random_state = int(random_state)
        self.pipeline: Pipeline = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        random_state=self.random_state,
                        max_iter=1000,
                        class_weight="balanced",
                    ),
                ),
            ]
        )
        self.is_fitted: bool = False
        self.metadata: Dict[str, object] = {
            "feature_columns": list(TOP_FEATURE_COLUMNS),
            "threshold_buy": self.threshold_buy,
            "threshold_sell": self.threshold_sell,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def fit(self, rows: Sequence[TopFeatureRow | Dict[str, object]]) -> Dict[str, object]:
        """训练模型并返回训练摘要。"""
        df = self._rows_to_frame(rows=rows, require_label=True)
        if df.empty:
            raise ValueError("训练样本为空")

        x = df[TOP_FEATURE_COLUMNS].astype(float)
        y = df["label"].astype(int)
        self.pipeline.fit(x, y)
        self.is_fitted = True

        prob = self.pipeline.predict_proba(x)[:, 1]
        pred = (prob >= 0.5).astype(int)
        summary = {
            "sample_count": int(len(df)),
            "positive_ratio": float(y.mean()) if len(y) > 0 else 0.0,
            "accuracy": float(accuracy_score(y, pred)),
            "auc": float(roc_auc_score(y, prob)) if len(set(y.tolist())) > 1 else 0.5,
            "feature_columns": list(TOP_FEATURE_COLUMNS),
            "report": classification_report(y, pred, output_dict=True, zero_division=0),
        }
        self.metadata["last_fit_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.metadata["train_summary"] = {
            "sample_count": summary["sample_count"],
            "positive_ratio": round(summary["positive_ratio"], 4),
            "accuracy": round(summary["accuracy"], 4),
            "auc": round(summary["auc"], 4),
        }
        return summary

    def predict(self, rows: Sequence[TopFeatureRow | Dict[str, object]]) -> List[TopPrediction]:
        """批量预测 Top 风险。"""
        self._ensure_fitted()
        df = self._rows_to_frame(rows=rows, require_label=False)
        if df.empty:
            return []

        x = df[TOP_FEATURE_COLUMNS].astype(float)
        prob = self.pipeline.predict_proba(x)[:, 1]
        predictions: List[TopPrediction] = []
        for index, row in df.reset_index(drop=True).iterrows():
            top_prob = float(prob[index])
            predictions.append(
                TopPrediction(
                    symbol=str(row.get("symbol", "") or ""),
                    trade_date=str(row.get("trade_date", "") or ""),
                    top_prob=top_prob,
                    decision=self._decision_from_prob(top_prob),
                    threshold_buy=self.threshold_buy,
                    threshold_sell=self.threshold_sell,
                )
            )
        return predictions

    def predict_one(self, row: TopFeatureRow | Dict[str, object]) -> TopPrediction:
        """预测单条样本。"""
        results = self.predict([row])
        if not results:
            raise ValueError("预测失败：输入样本为空")
        return results[0]

    def save(self, path: str) -> str:
        """保存模型到本地。"""
        self._ensure_fitted()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        payload = {
            "pipeline": self.pipeline,
            "metadata": self.metadata,
            "is_fitted": self.is_fitted,
        }
        with open(path, "wb") as f:
            pickle.dump(payload, f)
        return path

    @classmethod
    def load(cls, path: str) -> "TopPredictModel":
        """从本地加载模型。"""
        with open(path, "rb") as f:
            payload = pickle.load(f)
        model = cls(
            threshold_buy=float(payload.get("metadata", {}).get("threshold_buy", 0.4) or 0.4),
            threshold_sell=float(payload.get("metadata", {}).get("threshold_sell", 0.6) or 0.6),
        )
        model.pipeline = payload["pipeline"]
        model.metadata = dict(payload.get("metadata", {}) or {})
        model.is_fitted = bool(payload.get("is_fitted", True))
        return model

    def get_feature_importance(self) -> Dict[str, float]:
        """获取逻辑回归系数，作为可解释特征贡献。"""
        self._ensure_fitted()
        estimator: LogisticRegression = self.pipeline.named_steps["model"]
        coef = estimator.coef_[0]
        return {
            feature: round(float(weight), 6)
            for feature, weight in zip(TOP_FEATURE_COLUMNS, coef)
        }

    def _decision_from_prob(self, top_prob: float) -> str:
        """根据概率输出策略决策建议。"""
        if top_prob > self.threshold_sell:
            return "sell"
        if top_prob < self.threshold_buy:
            return "buy_or_hold"
        return "watch"

    def _rows_to_frame(
        self,
        rows: Sequence[TopFeatureRow | Dict[str, object]],
        require_label: bool,
    ) -> pd.DataFrame:
        """将输入样本转换为 DataFrame。"""
        normalized_rows: List[Dict[str, object]] = []
        for row in rows:
            if isinstance(row, TopFeatureRow):
                item = row.to_dict()
            else:
                item = dict(row or {})
            normalized = {
                "trade_date": str(item.get("trade_date", "") or ""),
                "symbol": str(item.get("symbol", "") or ""),
            }
            for feature in TOP_FEATURE_COLUMNS:
                normalized[feature] = float(item.get(feature, 0.0) or 0.0)
            if "label" in item and item.get("label") is not None:
                normalized["label"] = int(item.get("label", 0) or 0)
            elif require_label:
                raise ValueError(f"训练样本缺少 label: {item}")
            normalized_rows.append(normalized)
        df = pd.DataFrame(normalized_rows)
        if df.empty:
            return pd.DataFrame(columns=["trade_date", "symbol", *TOP_FEATURE_COLUMNS, "label"])
        return df

    def _ensure_fitted(self) -> None:
        """确保模型已训练。"""
        if not self.is_fitted:
            raise RuntimeError("TopPredictModel 尚未训练，请先调用 fit()")


def build_bootstrap_top_rows() -> List[TopFeatureRow]:
    """构建用于冷启动的 Top 风险先验样本。"""
    return [
        TopFeatureRow("20260301", "BOOT001", 0.82, 0.22, 0.68, 0.08, 0.06, 0.12, 0),
        TopFeatureRow("20260302", "BOOT002", 0.76, 0.28, 0.61, 0.12, 0.10, 0.08, 0),
        TopFeatureRow("20260303", "BOOT003", 0.71, 0.35, 0.56, 0.16, 0.12, 0.05, 0),
        TopFeatureRow("20260304", "BOOT004", 0.66, 0.42, 0.51, 0.22, 0.18, 0.01, 0),
        TopFeatureRow("20260305", "BOOT005", 0.64, 0.56, 0.47, 0.30, 0.24, -0.02, 1),
        TopFeatureRow("20260306", "BOOT006", 0.60, 0.63, 0.44, 0.36, 0.32, -0.03, 1),
        TopFeatureRow("20260307", "BOOT007", 0.57, 0.72, 0.38, 0.45, 0.39, -0.06, 1),
        TopFeatureRow("20260308", "BOOT008", 0.52, 0.78, 0.35, 0.54, 0.47, -0.09, 1),
        TopFeatureRow("20260309", "BOOT009", 0.80, 0.31, 0.63, 0.11, 0.09, 0.10, 0),
        TopFeatureRow("20260310", "BOOT010", 0.74, 0.38, 0.58, 0.18, 0.14, 0.04, 0),
        TopFeatureRow("20260311", "BOOT011", 0.59, 0.69, 0.41, 0.44, 0.36, -0.05, 1),
        TopFeatureRow("20260312", "BOOT012", 0.50, 0.82, 0.32, 0.60, 0.52, -0.10, 1),
    ]


def load_or_build_top_model(path: str = DEFAULT_TOP_MODEL_PATH) -> TopPredictModel:
    """加载已有 Top 模型，缺失时用冷启动样本训练并保存。"""
    model_path = str(path or DEFAULT_TOP_MODEL_PATH)
    try:
        if os.path.exists(model_path):
            model = TopPredictModel.load(model_path)
            logger.info(f"加载 Top 风险模型: {model_path}")
            return model
    except Exception as exc:
        logger.warning(f"加载 Top 风险模型失败，转冷启动模型: {exc}")

    model = TopPredictModel()
    model.fit(build_bootstrap_top_rows())
    try:
        model.save(model_path)
        logger.info(f"冷启动 Top 风险模型已生成: {model_path}")
    except Exception as exc:
        logger.warning(f"保存冷启动 Top 风险模型失败: {exc}")
    return model
