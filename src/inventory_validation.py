"""Валидация схемы и качества батча данных складских запасов."""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"

REQUIRED_COLUMNS = ["store_id", "sku_id", "date", "sales_qty", "stock_qty"]
MIN_ROWS = 100
MAX_NULL_RATE = 0.05


def validate(df: pd.DataFrame) -> dict:
    """Возвращает словарь с результатами валидации."""
    results = {
        "timestamp": datetime.now().isoformat(),
        "n_rows": len(df),
        "passed": True,
        "errors": [],
        "warnings": [],
    }

    # Проверка обязательных столбцов
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        results["errors"].append(f"Отсутствуют столбцы: {missing}")
        results["passed"] = False

    # Минимальное количество строк
    if len(df) < MIN_ROWS:
        results["errors"].append(f"Слишком мало строк: {len(df)} < {MIN_ROWS}")
        results["passed"] = False

    # Доля пропусков
    for col in REQUIRED_COLUMNS:
        if col in df.columns:
            null_rate = df[col].isna().mean()
            if null_rate > MAX_NULL_RATE:
                results["errors"].append(
                    f"Высокая доля пропусков в '{col}': {null_rate:.1%}"
                )
                results["passed"] = False

    # Отрицательные значения
    for col in ["sales_qty", "stock_qty"]:
        if col in df.columns:
            n_neg = (df[col] < 0).sum()
            if n_neg > 0:
                results["warnings"].append(
                    f"Обнаружено {n_neg} отрицательных значений в '{col}'"
                )

    return results


def validate_and_report(df: pd.DataFrame) -> dict:
    results = validate(df)
    _write_report(results)
    if not results["passed"]:
        raise ValueError(f"Валидация провалена: {results['errors']}")
    print(f"[validation] ✅ Валидация пройдена ({results['n_rows']} строк)")
    return results


def _write_report(results: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "data_validation.md"
    status = "✅ PASSED" if results["passed"] else "❌ FAILED"
    errors_md = "\n".join(f"- ❌ {e}" for e in results["errors"]) or "- нет"
    warnings_md = "\n".join(f"- ⚠️ {w}" for w in results["warnings"]) or "- нет"
    content = f"""# Отчёт валидации данных

**Время:** {results['timestamp']}
**Статус:** {status}
**Строк в батче:** {results['n_rows']}

## Ошибки
{errors_md}

## Предупреждения
{warnings_md}
"""
    path.write_text(content, encoding="utf-8")
    print(f"[validation] Отчёт сохранён → {path}")


if __name__ == "__main__":
    from inventory_data import load_batch
    df = load_batch()
    validate_and_report(df)
