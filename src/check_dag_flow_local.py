"""Локальная проверка логики DAG без запуска Airflow.

Проверяем сценарий:
1. Есть incoming batch за 2026-06-13.
2. Загружаем batch.
3. Валидируем данные.
4. Проверяем CT-условия.
5. Убеждаемся, что shock batch требует retraining.
"""

from pathlib import Path
import pandas as pd

from inventory_validation import validate_and_report
from inventory_evaluate import evaluate_current_model_on_batch
from inventory_train import train


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BATCHES_DIR = DATA_DIR / "daily_batches"
INCOMING_DIR = DATA_DIR / "incoming"


def load_days(start_day: int, end_day: int) -> pd.DataFrame:
    frames = []
    for day in range(start_day, end_day + 1):
        date = f"2026-06-{day:02d}"
        frames.append(pd.read_csv(BATCHES_DIR / date / "inventory.csv"))
    return pd.concat(frames, ignore_index=True)


def main():
    batch_date = "2026-06-13"
    batch_path = INCOMING_DIR / batch_date / "inventory.csv"
    metadata_path = INCOMING_DIR / batch_date / "metadata.json"

    if not batch_path.exists():
        raise FileNotFoundError(
            f"Incoming batch not found: {batch_path}. "
            f"Run: python src\\simulate_batch_arrival.py --clear --date {batch_date}"
        )

    print("1. Обучаем текущую production-модель на спокойной истории 01.06–12.06")
    history_df = load_days(1, 12)
    train_metrics = train(history_df)
    print(train_metrics)

    print()
    print(f"2. Загружаем incoming batch: {batch_path}")
    batch_df = pd.read_csv(batch_path)
    print(batch_df.head())
    print(f"rows={len(batch_df)}")

    print()
    print("3. Проверяем metadata")
    if metadata_path.exists():
        print(metadata_path.read_text(encoding="utf-8"))
    else:
        print("metadata.json не найден")

    print()
    print("4. Валидируем данные")
    validation_result = validate_and_report(batch_df)
    print(validation_result)

    print()
    print("5. Проверяем текущую модель на новом batch-е")
    current_check = evaluate_current_model_on_batch(batch_df)
    print(current_check)

    print()
    if current_check["retrain_required"]:
        print("Итог: retraining должен быть запущен ✅")
    else:
        print("Итог: retraining можно пропустить ⬜")


if __name__ == "__main__":
    main()