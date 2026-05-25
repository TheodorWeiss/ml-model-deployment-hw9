"""Загрузка и генерация синтетических данных складских запасов."""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPORTS_DIR = REPO_ROOT / "reports"


def generate_inventory_batch(
    n_skus: int = 50,
    n_days: int = 30,
    n_stores: int = 3,
    seed: int = 42,
    output_path: str | None = None,
) -> pd.DataFrame:
    """Генерирует синтетический батч данных о складских остатках.

    Модель: остатки убывают линейно + шум. Продажи ~ Poisson(λ).
    """
    rng = np.random.default_rng(seed)
    base_date = datetime(2026, 4, 1)

    rows = []
    for store_id in range(1, n_stores + 1):
        for sku_id in range(1, n_skus + 1):
            initial_stock = rng.integers(200, 1000)
            daily_sales_mean = rng.uniform(5, 30)
            for day in range(n_days):
                current_date = base_date + timedelta(days=day)
                sales_qty = int(rng.poisson(daily_sales_mean))
                stock_qty = max(0, int(initial_stock - daily_sales_mean * day + rng.normal(0, 10)))
                rows.append(
                    {
                        "store_id": store_id,
                        "sku_id": sku_id,
                        "date": current_date.strftime("%Y-%m-%d"),
                        "sales_qty": sales_qty,
                        "stock_qty": stock_qty,
                        "price": round(rng.uniform(50, 5000), 2),
                    }
                )

    df = pd.DataFrame(rows)

    if output_path is None:
        output_path = DATA_DIR / "demo_inventory_batch.csv"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[inventory_data] Сохранено {len(df)} строк → {output_path}")
    return df


def load_batch(path: str | None = None) -> pd.DataFrame:
    if path is None:
        path = DATA_DIR / "demo_inventory_batch.csv"
    df = pd.read_csv(path, parse_dates=["date"])
    print(f"[inventory_data] Загружено {len(df)} строк из {path}")
    return df


def log_sensor_event(source: str, n_rows: int, n_receipts: int) -> None:
    """Записывает событие появления нового батча в лог-файл."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = REPORTS_DIR / "airflow_sensor_log.md"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = (
        f"| {ts} | {source} | {n_rows} строк | {n_receipts:,} чеков |"
        f" {'✅ >10M' if n_receipts > 10_000_000 else '⬜ <10M'} |\n"
    )
    header = (
        "# Лог сенсора данных (S3 / FileSensor)\n\n"
        "| Время | Источник | Строк | Чеков | Порог 10M |\n"
        "|---|---|---|---|---|\n"
    )
    if not log_path.exists():
        log_path.write_text(header)
    with open(log_path, "a") as f:
        f.write(entry)


if __name__ == "__main__":
    df = generate_inventory_batch()
    print(df.head())
    print(df.shape)
