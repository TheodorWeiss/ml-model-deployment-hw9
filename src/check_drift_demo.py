"""Проверка деградации текущей модели на shock batch 2026-06-13."""

from pathlib import Path
import numpy as np
import pandas as pd

from inventory_train import train, load_model, build_features

REPO_ROOT = Path(__file__).resolve().parent.parent
BATCHES_DIR = REPO_ROOT / "data" / "daily_batches"


def load_days(start_day: int, end_day: int) -> pd.DataFrame:
    frames = []
    for day in range(start_day, end_day + 1):
        date = f"2026-06-{day:02d}"
        path = BATCHES_DIR / date / "inventory.csv"
        frames.append(pd.read_csv(path))
    return pd.concat(frames, ignore_index=True)


def evaluate_model_on_batch(model, df: pd.DataFrame, label: str) -> dict:
    X, y = build_features(df)
    y_pred = model.predict(X)

    rmse = float(np.sqrt(np.mean((y.values - y_pred) ** 2)))

    smape = float(
        np.mean(
            2 * np.abs(y.values - y_pred)
            / (np.abs(y.values) + np.abs(y_pred) + 1e-9)
        )
        * 100
    )

    tol = 0.15
    accuracy_proxy = float(
        np.mean(np.abs(y.values - y_pred) <= tol * (np.abs(y.values) + 1))
    )

    print()
    print(f"=== {label} ===")
    print(f"rows: {len(df)}")
    print(f"RMSE: {rmse:.2f}")
    print(f"sMAPE: {smape:.2f}%")
    print(f"accuracy_proxy: {accuracy_proxy:.3f}")

    return {
        "label": label,
        "rmse": rmse,
        "smape": smape,
        "accuracy_proxy": accuracy_proxy,
    }


if __name__ == "__main__":
    print("Обучаем production-модель на спокойной истории: 2026-06-01 ... 2026-06-12")
    history_df = load_days(1, 12)
    train_metrics = train(history_df)
    print("Train metrics:", train_metrics)

    model = load_model()

    normal_batch = pd.read_csv(BATCHES_DIR / "2026-06-12" / "inventory.csv")
    shock_batch = pd.read_csv(BATCHES_DIR / "2026-06-13" / "inventory.csv")
    post_shock_batch = pd.read_csv(BATCHES_DIR / "2026-06-14" / "inventory.csv")

    evaluate_model_on_batch(model, normal_batch, "Normal batch: 2026-06-12")
    evaluate_model_on_batch(model, shock_batch, "Shock batch: 2026-06-13")
    evaluate_model_on_batch(model, post_shock_batch, "Post-shock batch: 2026-06-14")