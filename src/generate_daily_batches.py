"""Генерация daily batch-ей для HW9.

Сценарий:
- 15 ежедневных batch-ей с 2026-06-01
- каждый batch имитирует ежедневную выгрузку складских данных
- на 13-й день заложен demand shock: резкий рост спроса на часть SKU
- цель модели: предсказать stock_qty_next_day
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BATCHES_DIR = DATA_DIR / "daily_batches"
INCOMING_DIR = DATA_DIR / "incoming"

START_DATE = datetime(2026, 6, 1)
N_DAYS = 15
N_STORES = 4
N_SKUS = 80
SEED = 42

SHOCK_DAY_INDEX = 13  # 13-й batch: 2026-06-13


def generate_daily_batches() -> None:
    rng = np.random.default_rng(SEED)

    if BATCHES_DIR.exists():
        shutil.rmtree(BATCHES_DIR)
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)

    stores = [f"store_{i}" for i in range(1, N_STORES + 1)]
    skus = [f"sku_{i:03d}" for i in range(1, N_SKUS + 1)]

    # Базовые характеристики SKU
    sku_base_sales = {sku: rng.uniform(4, 25) for sku in skus}
    sku_price = {sku: round(rng.uniform(50, 5000), 2) for sku in skus}

    # Начальные остатки по магазин-SKU
    stock = {
        (store, sku): int(rng.integers(80, 350))
        for store in stores
        for sku in skus
    }

    # SKU, на которых случится всплеск спроса
    shock_skus = set(rng.choice(skus, size=18, replace=False))

    all_rows = []

    for day_index in range(1, N_DAYS + 1):
        batch_date = START_DATE + timedelta(days=day_index - 1)
        batch_date_str = batch_date.strftime("%Y-%m-%d")

        is_shock_day = day_index == SHOCK_DAY_INDEX
        is_post_shock = day_index > SHOCK_DAY_INDEX

        rows = []
        total_receipts = int(rng.integers(900_000, 1_800_000))

        # Для 13-го дня делаем порог задания >10M чеков естественно через metadata
        if is_shock_day:
            total_receipts = int(rng.integers(11_500_000, 13_500_000))

        for store in stores:
            for sku in skus:
                current_stock = stock[(store, sku)]
                base_sales = sku_base_sales[sku]

                promo_flag = 0
                demand_multiplier = 1.0
                event_type = "normal"

                # Небольшая регулярная недельная сезонность
                if batch_date.weekday() in [4, 5]:  # пятница/суббота
                    demand_multiplier *= 1.15

                # Иногда обычные промо
                if rng.random() < 0.08:
                    promo_flag = 1
                    demand_multiplier *= rng.uniform(1.2, 1.6)

                # Главное событие на 13-й день
                if is_shock_day and sku in shock_skus:
                    promo_flag = 1
                    event_type = "demand_shock_promo_supply_disruption"
                    demand_multiplier *= rng.uniform(6.0, 9.0)

                # После 13-го дня спрос частично остаётся повышенным
                if is_post_shock and sku in shock_skus:
                    event_type = "post_shock_tail"
                    demand_multiplier *= rng.uniform(1.8, 2.6)

                expected_sales = base_sales * demand_multiplier
                sales_qty = int(rng.poisson(expected_sales))
                sales_qty = min(sales_qty, current_stock)

                # Поставки: не каждый день, чаще при низком остатке
                delivery_qty = 0

                # На shock-day для выбранных SKU имитируем задержку поставок:
                # спрос резко вырос, но склад не успел пополниться.
                if is_shock_day and sku in shock_skus:
                    delivery_qty = 0
                else:
                    if current_stock < 90 or rng.random() < 0.18:
                        delivery_qty = int(rng.integers(30, 160))

                stock_qty_next_day = max(
                    0,
                    current_stock - sales_qty + delivery_qty
                )

                row = {
                    "date": batch_date_str,
                    "store_id": store,
                    "sku_id": sku,
                    "stock_qty": current_stock,
                    "sales_qty": sales_qty,
                    "delivery_qty": delivery_qty,
                    "price": sku_price[sku],
                    "promo_flag": promo_flag,
                    "day_of_week": batch_date.weekday(),
                    "event_type": event_type,
                    "demand_multiplier": round(demand_multiplier, 3),
                    "stock_qty_next_day": stock_qty_next_day,
                }

                rows.append(row)
                all_rows.append(row)

                # Обновляем состояние склада для следующего дня
                stock[(store, sku)] = stock_qty_next_day

        batch_df = pd.DataFrame(rows)

        batch_dir = BATCHES_DIR / batch_date_str
        batch_dir.mkdir(parents=True, exist_ok=True)

        batch_df.to_csv(batch_dir / "inventory.csv", index=False)

        metadata = {
            "batch_date": batch_date_str,
            "day_index": day_index,
            "n_rows": len(batch_df),
            "total_receipts": total_receipts,
            "is_shock_day": is_shock_day,
            "event": "demand_shock_promo" if is_shock_day else "normal",
            "description": (
                "Demand shock: strong promo-driven sales spike for selected SKUs"
                if is_shock_day
                else "Regular daily inventory batch"
            ),
        }

        (batch_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(
            f"[batch] {batch_date_str}: {len(batch_df)} rows, "
            f"receipts={total_receipts:,}, event={metadata['event']}"
        )

    full_df = pd.DataFrame(all_rows)
    full_df.to_csv(DATA_DIR / "inventory_full_history.csv", index=False)

    print()
    print(f"Готово: {N_DAYS} daily batch-ей сохранены в {BATCHES_DIR}")
    print(f"Полная история сохранена в {DATA_DIR / 'inventory_full_history.csv'}")


if __name__ == "__main__":
    generate_daily_batches()