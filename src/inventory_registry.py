"""Локальный реестр моделей (аналог MLflow Model Registry).

Хранит JSON-файл reports/local_registry.json.
Каждая запись описывает одну версию модели со всеми метриками и решением.
"""

import json
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"
REGISTRY_PATH = REPORTS_DIR / "local_registry.json"


def register(
    metrics: dict,
    stage: str = "production_candidate",
    source_data: str = "data/demo_inventory_batch.csv",
    reason: str = "",
) -> dict:
    """Добавляет запись о модели в локальный реестр."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    registry = _load_registry()
    version = len(registry) + 1

    entry = {
        "model_name": "inventory_stock_forecaster",
        "version": version,
        "stage": stage,
        "metrics": metrics,
        "registered_at": datetime.now().isoformat(),
        "source_data": source_data,
        "decision_reason": reason,
    }
    registry.append(entry)
    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2))

    _write_registry_log(entry)
    print(f"[registry] Зарегистрирована версия v{version} (stage={stage})")
    return entry


def promote_to_production(version: int) -> None:
    """Переводит указанную версию в stage='production'."""
    registry = _load_registry()
    for entry in registry:
        if entry["version"] == version:
            entry["stage"] = "production"
            entry["promoted_at"] = datetime.now().isoformat()
    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2))
    print(f"[registry] Версия v{version} переведена в production")


def get_latest_production() -> dict | None:
    registry = _load_registry()
    prod = [e for e in registry if e.get("stage") == "production"]
    return sorted(prod, key=lambda e: e.get("registered_at", ""))[-1] if prod else None


def _load_registry() -> list:
    if REGISTRY_PATH.exists():
        return json.loads(REGISTRY_PATH.read_text())
    return []


def _write_registry_log(entry: dict) -> None:
    path = REPORTS_DIR / "airflow_registry_log.md"
    ts = entry["registered_at"]
    v = entry["version"]
    stage = entry["stage"]
    acc = entry["metrics"].get("accuracy_proxy", "—")
    mape = entry["metrics"].get("mape_pct", "—")
    reason = entry["decision_reason"]

    header = (
        "# Лог регистрации моделей\n\n"
        "| Время | Версия | Stage | accuracy_proxy | MAPE% | Причина |\n"
        "|---|---|---|---|---|---|\n"
    )
    row = f"| {ts} | v{v} | {stage} | {acc} | {mape} | {reason} |\n"

    if not path.exists():
        path.write_text(header, encoding="utf-8")
    with open(path, "a", encoding="utf-8") as f:
        f.write(row)


if __name__ == "__main__":
    from inventory_data import load_batch
    from inventory_train import train
    from inventory_evaluate import evaluate, compare_with_baseline

    df = load_batch()
    metrics = train(df)
    metrics = evaluate(metrics)
    decision = compare_with_baseline(metrics)

    if decision == "register_model":
        entry = register(metrics, stage="production", reason="Автоматический CT-цикл")
        promote_to_production(entry["version"])
    else:
        print("[registry] Регистрация пропущена — модель не прошла quality gate")
