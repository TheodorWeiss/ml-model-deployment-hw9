"""Оценка модели и сравнение с базовой версией (quality gate)."""

import json
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"

# Порог CT-цикла (совпадает с заданием): если accuracy prod-модели < 0.85 → переобучаем.
# Здесь используется для quality gate новой модели с более мягким порогом.
ACCURACY_THRESHOLD = 0.75  # quality gate: ±50%-точность >= 75% → деплой разрешён
MAPE_THRESHOLD = 50.0       # sMAPE <= 50% → приемлемо для складских остатков


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
    print(f"[evaluate] Quality gate: {status}  (acc={acc:.3f}, MAPE={mape:.2f}%)")
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
        result["reason"] = "Модель не прошла quality gate (accuracy или MAPE)"
    elif new_acc >= baseline_acc:
        result["decision"] = "register_model"
        result["reason"] = f"Новая модель лучше или равна baseline ({new_acc:.3f} >= {baseline_acc:.3f})"
    else:
        result["decision"] = "skip_deploy"
        result["reason"] = f"Новая модель хуже baseline ({new_acc:.3f} < {baseline_acc:.3f})"

    _write_compare_log(result)
    print(f"[compare] Решение: {result['decision']} — {result['reason']}")
    return result["decision"]


def _load_baseline_accuracy(registry_path) -> float:
    try:
        registry = json.loads(Path(registry_path).read_text())
        prod_entries = [e for e in registry if e.get("stage") == "production"]
        if prod_entries:
            latest = sorted(prod_entries, key=lambda e: e.get("registered_at", ""))[-1]
            return latest.get("metrics", {}).get("accuracy_proxy", 0.0)
    except Exception:
        pass
    return 0.0  # нет baseline → любая модель лучше


def _write_metrics_report(metrics: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "model_metrics.md"
    gate = "✅ PASSED" if metrics.get("passes_quality_gate") else "❌ FAILED"
    content = f"""# Метрики модели

**Время:** {metrics.get('evaluated_at', '')}
**Quality gate:** {gate}

| Метрика | Значение | Порог |
|---|---|---|
| accuracy_proxy | {metrics.get('accuracy_proxy', 0):.4f} | ≥ {metrics.get('accuracy_threshold', 0.85)} |
| MAPE, % | {metrics.get('mape_pct', 0):.2f} | ≤ {metrics.get('mape_threshold', 20)} |
| RMSE | {metrics.get('rmse', 0):.2f} | — |
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
**Решение:** {result['decision']}
**Причина:** {result['reason']}

| | Значение |
|---|---|
| Новая модель accuracy | {result['new_accuracy']:.4f} |
| Baseline accuracy | {result['baseline_accuracy']:.4f} |
| Quality gate | {'✅' if result['passes_quality_gate'] else '❌'} |
"""
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    from inventory_data import load_batch
    from inventory_train import train
    df = load_batch()
    metrics = train(df)
    metrics = evaluate(metrics)
    decision = compare_with_baseline(metrics)
    print("Итоговое решение:", decision)
