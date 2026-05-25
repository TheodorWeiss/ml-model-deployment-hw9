"""Оценка модели и сравнение с базовой версией (quality gate)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"

# Единый порог из задания:
# accuracy текущей/новой модели ниже 0.85 считается неприемлемой.
ACCURACY_THRESHOLD = 0.85

# Дополнительный sanity-check: ошибка не должна быть слишком высокой.
MAPE_THRESHOLD = 35.0


def evaluate(metrics: dict) -> dict:
    """Расширяет словарь метрик решением о качестве."""
    acc = metrics.get("accuracy_proxy", 0.0)
    mape = metrics.get("mape_pct", 100.0)

    passes_gate = acc >= ACCURACY_THRESHOLD and mape <= MAPE_THRESHOLD

    metrics["passes_quality_gate"] = passes_gate
    metrics["accuracy_threshold"] = ACCURACY_THRESHOLD
    metrics["mape_threshold"] = MAPE_THRESHOLD
    metrics["evaluated_at"] = datetime.now().isoformat()

    _write_metrics_report(metrics)

    status = "✅ PASSED" if passes_gate else "❌ FAILED"
    print(f"[evaluate] Quality gate: {status}  (acc={acc:.3f}, sMAPE={mape:.2f}%)")

    return metrics


def compare_with_baseline(new_metrics: dict, registry_path: str | None = None) -> str:
    """Сравнивает новую модель с production-baseline из реестра.

    Возвращает 'register_model' или 'skip_deploy'.
    """
    if registry_path is None:
        registry_path = REPORTS_DIR / "local_registry.json"

    baseline_acc = _load_baseline_accuracy(registry_path)
    new_acc = new_metrics.get("accuracy_proxy", 0.0)

    result = {
        "timestamp": datetime.now().isoformat(),
        "new_accuracy": new_acc,
        "baseline_accuracy": baseline_acc,
        "passes_quality_gate": new_metrics.get("passes_quality_gate", False),
        "decision": None,
        "reason": None,
    }

    if not new_metrics.get("passes_quality_gate", False):
        result["decision"] = "skip_deploy"
        result["reason"] = (
            f"Модель не прошла quality gate: "
            f"accuracy={new_acc:.3f}, threshold={ACCURACY_THRESHOLD:.2f}"
        )
    elif new_acc >= baseline_acc:
        result["decision"] = "register_model"
        result["reason"] = (
            f"Новая модель лучше или равна baseline "
            f"({new_acc:.3f} >= {baseline_acc:.3f})"
        )
    else:
        result["decision"] = "skip_deploy"
        result["reason"] = (
            f"Новая модель хуже baseline "
            f"({new_acc:.3f} < {baseline_acc:.3f})"
        )

    _write_compare_log(result)

    print(f"[compare] Решение: {result['decision']} — {result['reason']}")
    return result["decision"]

def evaluate_current_model_on_batch(df, model_path=None) -> dict:
    """Оценивает текущую production-модель на новом batch-е.

    Эта функция нужна для CT-триггера:
    если текущая модель плохо работает на новом batch-е,
    Airflow запускает переобучение.
    """
    import pickle
    import numpy as np
    from inventory_train import build_features, MODEL_PATH

    if model_path is None:
        model_path = MODEL_PATH

    model_path = Path(model_path)

    if not model_path.exists():
        print("[current_model] Production model not found. Retraining is required.")
        return {
            "current_accuracy": 0.0,
            "current_rmse": None,
            "current_smape": None,
            "model_exists": False,
            "retrain_required": True,
            "reason": "production_model_not_found",
        }

    with open(model_path, "rb") as f:
        model = pickle.load(f)

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

    result = {
        "current_accuracy": round(accuracy_proxy, 4),
        "current_rmse": round(rmse, 4),
        "current_smape": round(smape, 4),
        "model_exists": True,
        "retrain_required": accuracy_proxy < ACCURACY_THRESHOLD,
        "reason": (
            "model_accuracy_below_threshold"
            if accuracy_proxy < ACCURACY_THRESHOLD
            else "model_accuracy_ok"
        ),
    }

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "current_model_check.md"
    path.write_text(
        f"""# Проверка текущей production-модели на новом batch-е

| Метрика | Значение | Порог |
|---|---:|---:|
| current_accuracy | {result['current_accuracy']:.4f} | ≥ {ACCURACY_THRESHOLD:.2f} |
| current_sMAPE, % | {result['current_smape']:.2f} | — |
| current_RMSE | {result['current_rmse']:.2f} | — |

**Retraining required:** `{result['retrain_required']}`  
**Reason:** `{result['reason']}`
""",
        encoding="utf-8",
    )

    print(
        f"[current_model] acc={accuracy_proxy:.3f}, "
        f"sMAPE={smape:.2f}%, retrain={result['retrain_required']}"
    )

    return result

def _load_baseline_accuracy(registry_path) -> float:
    try:
        registry = json.loads(Path(registry_path).read_text(encoding="utf-8"))
        prod_entries = [e for e in registry if e.get("stage") == "production"]
        if prod_entries:
            latest = sorted(prod_entries, key=lambda e: e.get("registered_at", ""))[-1]
            return latest.get("metrics", {}).get("accuracy_proxy", 0.0)
    except Exception:
        pass

    return 0.0


def _write_metrics_report(metrics: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "model_metrics.md"

    gate = "✅ PASSED" if metrics.get("passes_quality_gate") else "❌ FAILED"

    content = f"""# Метрики модели

**Время:** {metrics.get('evaluated_at', '')}  
**Target:** `{metrics.get('target', '—')}`  
**Quality gate:** {gate}

| Метрика | Значение | Порог |
|---|---:|---:|
| accuracy_proxy | {metrics.get('accuracy_proxy', 0):.4f} | ≥ {metrics.get('accuracy_threshold', 0.85):.2f} |
| sMAPE, % | {metrics.get('mape_pct', 0):.2f} | ≤ {metrics.get('mape_threshold', 35):.2f} |
| RMSE | {metrics.get('rmse', 0):.2f} | — |
| Допуск accuracy_proxy | ±{metrics.get('tolerance_pct', 15)}% | — |
| Обучающих примеров | {metrics.get('n_train', 0)} | — |
| Валидационных примеров | {metrics.get('n_val', 0)} | — |
"""

    path.write_text(content, encoding="utf-8")
    print(f"[evaluate] Метрики → {path}")


def _write_compare_log(result: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "airflow_compare_log.md"

    content = f"""# Лог сравнения модели с baseline

**Время:** {result['timestamp']}  
**Решение:** `{result['decision']}`  
**Причина:** {result['reason']}

| | Значение |
|---|---:|
| Новая модель accuracy | {result['new_accuracy']:.4f} |
| Baseline accuracy | {result['baseline_accuracy']:.4f} |
| Quality gate | {'✅' if result['passes_quality_gate'] else '❌'} |
"""

    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    import pandas as pd
    from inventory_train import train

    df = pd.read_csv(REPO_ROOT / "data" / "daily_batches" / "2026-06-01" / "inventory.csv")
    metrics = train(df)
    metrics = evaluate(metrics)
    decision = compare_with_baseline(metrics)
    print("Итоговое решение:", decision)