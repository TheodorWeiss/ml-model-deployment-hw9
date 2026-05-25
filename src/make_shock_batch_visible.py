from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SHOCK_DATE = "2026-06-13"
BATCH_DIR = REPO_ROOT / "data" / "daily_batches" / SHOCK_DATE
CSV_PATH = BATCH_DIR / "inventory.csv"
METADATA_PATH = BATCH_DIR / "metadata.json"


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Batch file not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    if "stock_qty_next_day" not in df.columns:
        raise ValueError("Column 'stock_qty_next_day' not found")

    rng = np.random.default_rng(42)

    # Идея shock:
    # На 13-й день связь между признаками и target временно ломается:
    # например, сбой учета остатков, задержка отражения поставок,
    # пересортица или массовые корректировки склада.
    n = len(df)
    shock_mask = rng.random(n) < 0.80

    base = df["stock_qty_next_day"].to_numpy(dtype=float)

    # Создаем сильный шум, который нельзя нормально объяснить
    # через stock_qty / sales_qty / delivery_qty.
    random_factor = rng.uniform(0.15, 2.80, size=n)
    random_noise = rng.normal(loc=0, scale=np.maximum(base * 0.70, 30), size=n)

    shocked_target = base * random_factor + random_noise
    shocked_target = np.clip(shocked_target, 0, None).round().astype(int)

    df.loc[shock_mask, "stock_qty_next_day"] = shocked_target[shock_mask]

    # Дополнительно усилим скачки sales_qty, но это вторично.
    # Главное — сломать target, а не только sales.
    if "sales_qty" in df.columns:
        sales = df["sales_qty"].to_numpy(dtype=float)
        sales_factor = rng.uniform(0.2, 4.5, size=n)
        df.loc[shock_mask, "sales_qty"] = np.clip(
            sales[shock_mask] * sales_factor[shock_mask],
            0,
            None,
        ).round().astype(int)

    df.to_csv(CSV_PATH, index=False)

    metadata = {}
    if METADATA_PATH.exists():
        try:
            metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
        except Exception:
            metadata = {}

    metadata.update(
        {
            "date": SHOCK_DATE,
            "event": "shock",
            "shock_type": "stock_accounting_disruption",
            "shock_description": (
                "Synthetic stock accounting shock: relationship between "
                "features and stock_qty_next_day was intentionally disturbed "
                "to demonstrate performance degradation."
            ),
            "shock_rows_share": 0.80,
            "total_receipts": int(metadata.get("total_receipts", n * 300)),
        }
    )

    METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[shock] Updated {CSV_PATH}")
    print(f"[shock] Updated {METADATA_PATH}")
    print(f"[shock] Rows affected: {shock_mask.sum()} / {n}")


if __name__ == "__main__":
    main()