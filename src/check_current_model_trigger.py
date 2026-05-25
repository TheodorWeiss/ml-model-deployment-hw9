from pathlib import Path
import pandas as pd

from inventory_train import train, load_model
from inventory_evaluate import evaluate_current_model_on_batch

REPO_ROOT = Path(__file__).resolve().parent.parent
BATCHES_DIR = REPO_ROOT / "data" / "daily_batches"


def load_days(start_day: int, end_day: int) -> pd.DataFrame:
    frames = []
    for day in range(start_day, end_day + 1):
        date = f"2026-06-{day:02d}"
        frames.append(pd.read_csv(BATCHES_DIR / date / "inventory.csv"))
    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    # Сначала имитируем production-модель:
    # обучаем её на спокойной истории до события.
    history = load_days(1, 12)
    train(history)

    # Теперь проверяем эту production-модель на shock batch.
    shock_batch = pd.read_csv(BATCHES_DIR / "2026-06-13" / "inventory.csv")
    result = evaluate_current_model_on_batch(shock_batch)

    print(result)