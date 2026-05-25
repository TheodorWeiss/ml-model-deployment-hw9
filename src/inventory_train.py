"""Обучение модели прогноза складских запасов.

Модель умышленно простая (линейная регрессия) — задание сфокусировано
на оркестрации конвейера, а не на качестве самой модели.

Цель модели: предсказать остаток товара на следующий день:
    stock_qty_next_day
"""

from __future__ import annotations

import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"
MODEL_PATH = REPORTS_DIR / "model.pkl"

TARGET_COL = "stock_qty_next_day"


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix and target vector."""
    df = df.copy()

    if TARGET_COL not in df.columns:
        required_for_fallback = {"stock_qty", "delivery_qty", "sales_qty"}

        if required_for_fallback.issubset(df.columns):
            df[TARGET_COL] = (
                df["stock_qty"] + df["delivery_qty"] - df["sales_qty"]
            ).clip(lower=0)

            print(
                f"[inventory_train] Target column '{TARGET_COL}' was missing; "
                "created fallback target from stock_qty + delivery_qty - sales_qty."
            )
        else:
            missing = sorted(required_for_fallback - set(df.columns))
            raise ValueError(
                f"В данных нет целевого столбца '{TARGET_COL}', "
                f"и невозможно восстановить fallback-target. "
                f"Отсутствуют колонки: {missing}"
            )

    missing_features = [col for col in FEATURE_COLS if col not in df.columns]
    if missing_features:
        raise ValueError(f"В данных нет признаков: {missing_features}")

    X = df[FEATURE_COLS].copy()
    y = df[TARGET_COL].copy()

    return X, y


def train(df: pd.DataFrame, seed: int = 42) -> dict:
    """Обучает модель и возвращает метрики на validation-части."""
    X, y = build_features(df)

    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=seed,
    )

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_val)

    rmse = float(np.sqrt(np.mean((y_val.values - y_pred) ** 2)))

    smape = float(
        np.mean(
            2 * np.abs(y_val.values - y_pred)
            / (np.abs(y_val.values) + np.abs(y_pred) + 1e-9)
        )
        * 100
    )

    # accuracy_proxy: доля прогнозов в пределах ±15% от фактического остатка.
    # Это более строгая и понятная бизнес-метрика, чем прежние ±50%.
    tol = 0.15
    y_abs = np.abs(y_val.values)
    within_tol = np.mean(np.abs(y_val.values - y_pred) <= tol * (y_abs + 1))
    accuracy_proxy = float(within_tol)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    metrics = {
        "target": TARGET_COL,
        "rmse": round(rmse, 4),
        "mape_pct": round(smape, 4),
        "accuracy_proxy": round(accuracy_proxy, 4),
        "tolerance_pct": int(tol * 100),
        "n_train": len(X_train),
        "n_val": len(X_val),
        "trained_at": datetime.now().isoformat(),
        "model_path": str(MODEL_PATH),
    }

    print(
        f"[train] target={TARGET_COL} "
        f"RMSE={rmse:.2f}  sMAPE={smape:.2f}%  acc≈{accuracy_proxy:.3f}"
    )
    return metrics


def load_model():
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)


if __name__ == "__main__":
    df = pd.read_csv(REPO_ROOT / "data" / "daily_batches" / "2026-06-01" / "inventory.csv")
    metrics = train(df)
    print(metrics)