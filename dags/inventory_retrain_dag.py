"""DAG: inventory_retrain_pipeline

Continuous Training DAG для расчёта складских запасов сетевого магазина.

Вариант B: live-demo с реальным поступлением batch-ей.

Идея:
  - внешний producer публикует новые daily batch-и в data/incoming/
  - Airflow запускается каждую минуту
  - каждый DAG-run выбирает самый ранний необработанный batch
  - batch валидируется
  - текущая production-модель проверяется на новом batch-е
  - если CT-условия выполнены, запускается retraining
  - после завершения batch помечается как processed

CT-условия:
  1. Новый batch представляет ≥10M чеков по metadata.json
  2. accuracy текущей production-модели на новом batch-е < 0.85
  3. Срок действия production-модели истекает через <1 час
  4. В S3/MinIO появился новый дневной batch, который ещё не был обработан (это демонстрационное условие)

Ожидаемая структура данных:
  data/incoming/2026-06-01/inventory.csv
  data/incoming/2026-06-01/metadata.json
  data/incoming/2026-06-02/inventory.csv
  ...

Один DAG-run обрабатывает один новый batch.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

# Путь к src/ внутри контейнера Airflow
sys.path.insert(0, "/opt/airflow/src")

RECEIPTS_THRESHOLD = 10_000_000
ACCURACY_THRESHOLD = 0.85
MODEL_EXPIRATION_HOURS = 1

DATA_DIR = Path("/opt/airflow/data")
REPORTS_DIR = Path("/opt/airflow/reports")
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_BATCHES_PATH = REPORTS_DIR / "processed_batches.json"
MINIO_BUCKET = os.getenv("DZ9_MINIO_BUCKET", "inventory-batches")
AWS_CONN_ID = "aws_default"
CURRENT_BATCH_PATH = DATA_DIR / "current_inventory_batch.csv"
CURRENT_METADATA_PATH = DATA_DIR / "current_metadata.json"


# ─────────────────────────────────────────────────────────────
# Работа с очередью batch-ей
# ─────────────────────────────────────────────────────────────

def _read_processed_batches() -> list[str]:
    """Читает список уже обработанных batch-дат."""
    if not PROCESSED_BATCHES_PATH.exists():
        return []

    try:
        data = json.loads(PROCESSED_BATCHES_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def _write_processed_batches(processed: list[str]) -> None:
    """Сохраняет список уже обработанных batch-дат."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_BATCHES_PATH.write_text(
        json.dumps(sorted(set(processed)), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _list_inventory_batch_dates_from_s3() -> list[str]:
    """Возвращает список batch-дат, для которых в MinIO/S3 есть inventory.csv."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    keys = hook.list_keys(
        bucket_name=MINIO_BUCKET,
        prefix="incoming/",
    ) or []

    batch_dates = []

    for key in keys:
        # ожидаемый формат: incoming/2026-06-13/inventory.csv
        parts = key.split("/")

        if len(parts) == 3 and parts[0] == "incoming" and parts[2] == "inventory.csv":
            batch_dates.append(parts[1])

    return sorted(set(batch_dates))


def _find_next_unprocessed_batch() -> str | None:
    """Возвращает самую раннюю batch-дату, которая есть в S3, но ещё не обработана."""
    processed = set(_read_processed_batches())
    available = _list_inventory_batch_dates_from_s3()

    unprocessed = [batch_date for batch_date in available if batch_date not in processed]

    if not unprocessed:
        return None

    return sorted(unprocessed)[0]


def wait_for_new_inventory_batch(**context) -> bool:
    """PythonSensor: ждёт именно новый необработанный batch, а не просто любой файл в S3."""
    processed = set(_read_processed_batches())
    available = _list_inventory_batch_dates_from_s3()
    unprocessed = sorted([batch_date for batch_date in available if batch_date not in processed])

    print(f"[sensor] available batches in MinIO/S3: {available}")
    print(f"[sensor] processed batches: {sorted(processed)}")
    print(f"[sensor] unprocessed batches: {unprocessed}")

    if unprocessed:
        print(f"[sensor] Найден новый batch для обработки: {unprocessed[0]}")
        return True

    print("[sensor] Новых необработанных batch-ей пока нет")
    return False


def select_inventory_batch(**context):
    """Выбирает самый ранний необработанный batch в MinIO/S3 и пишет ключи в XCom."""
    batch_date = _find_next_unprocessed_batch()

    if not batch_date:
        raise AirflowSkipException(
            "Нет новых необработанных batch-ей в MinIO/S3 — пропускаем этот запуск"
        )

    inventory_key = f"incoming/{batch_date}/inventory.csv"
    metadata_key = f"incoming/{batch_date}/metadata.json"

    ti = context["ti"]
    ti.xcom_push(key="batch_date", value=batch_date)
    ti.xcom_push(key="inventory_key", value=inventory_key)
    ti.xcom_push(key="metadata_key", value=metadata_key)

    print(f"[select] Выбран batch: {batch_date}")
    print(f"[select] inventory key: s3://{MINIO_BUCKET}/{inventory_key}")
    print(f"[select] metadata key: s3://{MINIO_BUCKET}/{metadata_key}")

# ─────────────────────────────────────────────────────────────
# Загрузка и валидация
# ─────────────────────────────────────────────────────────────

def load_inventory_data(**context):
    import pandas as pd
    from inventory_data import log_sensor_event

    ti = context["ti"]

    batch_date = ti.xcom_pull(task_ids="select_inventory_batch", key="batch_date")
    inventory_key = ti.xcom_pull(task_ids="select_inventory_batch", key="inventory_key")
    metadata_key = ti.xcom_pull(task_ids="select_inventory_batch", key="metadata_key")

    if not batch_date or not inventory_key:
        raise RuntimeError("batch_date или inventory_key не найден в XCom")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    inventory_obj = hook.get_key(
        key=inventory_key,
        bucket_name=MINIO_BUCKET,
    )

    if inventory_obj is None:
        raise FileNotFoundError(f"S3 object not found: s3://{MINIO_BUCKET}/{inventory_key}")

    inventory_obj.download_file(str(CURRENT_BATCH_PATH))

    metadata = {}
    metadata_obj = hook.get_key(
        key=metadata_key,
        bucket_name=MINIO_BUCKET,
    )

    if metadata_obj is not None:
        metadata_obj.download_file(str(CURRENT_METADATA_PATH))
        try:
            metadata = json.loads(CURRENT_METADATA_PATH.read_text(encoding="utf-8"))
        except Exception:
            metadata = {}

    df = pd.read_csv(CURRENT_BATCH_PATH)

    total_receipts = int(metadata.get("total_receipts", len(df) * 300))
    batch_event = metadata.get("event", "unknown")

    log_sensor_event(
        f"s3://{MINIO_BUCKET}/{inventory_key}",
        len(df),
        total_receipts,
    )

    ti.xcom_push(key="batch_date", value=batch_date)
    ti.xcom_push(key="batch_path", value=str(CURRENT_BATCH_PATH))
    ti.xcom_push(key="metadata_path", value=str(CURRENT_METADATA_PATH))
    ti.xcom_push(key="inventory_key", value=inventory_key)
    ti.xcom_push(key="metadata_key", value=metadata_key)
    ti.xcom_push(key="n_rows", value=len(df))
    ti.xcom_push(key="total_receipts", value=total_receipts)
    ti.xcom_push(key="batch_event", value=batch_event)

    print(f"[load] Batch date: {batch_date}")
    print(f"[load] Downloaded: s3://{MINIO_BUCKET}/{inventory_key}")
    print(f"[load] Local copy: {CURRENT_BATCH_PATH}")
    print(f"[load] Загружено {len(df)} строк")
    print(f"[load] Event={batch_event}, receipts={total_receipts:,}")


def validate_inventory_data(**context):
    import pandas as pd
    from inventory_validation import validate_and_report

    ti = context["ti"]
    batch_path = ti.xcom_pull(task_ids="load_inventory_data", key="batch_path")

    df = pd.read_csv(batch_path)
    validate_and_report(df)

    _append_task_log("validate_inventory_data", "✅ Данные прошли валидацию")

def detect_batch_event(**context):
    """BranchPythonOperator: визуально показывает, является ли batch shock-событием."""
    ti = context["ti"]

    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")
    batch_event = ti.xcom_pull(task_ids="load_inventory_data", key="batch_event") or "unknown"

    message = f"batch={batch_date}; event={batch_event}"

    print(f"[event] {message}")
    _append_task_log("detect_batch_event", message)

    if batch_event == "shock":
        return "shock_alert"

    return "normal_batch_event"


def shock_alert(**context):
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")

    message = (
        f"⚠️ Shock batch detected: {batch_date}. "
        f"Expected behavior: quality_drop may become True and deployment may be skipped."
    )

    print(f"[shock_alert] {message}")
    _append_task_log("shock_alert", message)


def normal_batch_event(**context):
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")

    message = f"Normal batch event: {batch_date}"

    print(f"[normal_event] {message}")
    _append_task_log("normal_batch_event", message)

# ─────────────────────────────────────────────────────────────
# CT-условия
# ─────────────────────────────────────────────────────────────

def check_ct_conditions(**context):
    """BranchPythonOperator: решает, запускать retraining или нет.

    Условия Continuous Training:
    1. Пришёл новый дневной batch, который ещё не был обработан.
    2. В batch/metadata реально накопилось >= 10M чеков.
    3. Текущая модель показывает accuracy < 0.85 на новом batch-е.
    4. Срок действия модели истекает меньше чем через 1 час.

    Важно: 10M чеков остаётся отдельным бизнес-триггером,
    но не используется искусственно как замена факту появления нового batch-а.
    """
    import pandas as pd
    from inventory_evaluate import evaluate_current_model_on_batch

    ti = context["ti"]

    batch_path = ti.xcom_pull(task_ids="load_inventory_data", key="batch_path")
    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")
    total_receipts = ti.xcom_pull(task_ids="load_inventory_data", key="total_receipts") or 0
    batch_event = ti.xcom_pull(task_ids="load_inventory_data", key="batch_event") or "unknown"

    df = pd.read_csv(batch_path)

    processed_batches = _read_processed_batches()

    # 1. Новый дневной batch
    has_new_batch_date = batch_date not in processed_batches

    # 2. Отдельный бизнес-триггер: >= 10M чеков
    has_large_receipts_volume = total_receipts >= RECEIPTS_THRESHOLD

    # 3. Проверка качества текущей модели
    current_check = evaluate_current_model_on_batch(df)
    current_accuracy = current_check["current_accuracy"]
    has_quality_drop = current_check["retrain_required"]

    # 4. Проверка срока действия модели
    hours_left = _get_hours_until_expiration(REPORTS_DIR / "local_registry.json")
    model_expires_soon = hours_left <= MODEL_EXPIRATION_HOURS

    retrain_reasons = []

    if has_new_batch_date:
        retrain_reasons.append("new_batch_date")

    if has_large_receipts_volume:
        retrain_reasons.append("large_receipts_volume")

    if has_quality_drop:
        retrain_reasons.append("quality_drop")

    if model_expires_soon:
        retrain_reasons.append("model_expires_soon")

    should_retrain = len(retrain_reasons) > 0

    ti.xcom_push(key="current_accuracy", value=current_accuracy)
    ti.xcom_push(key="has_new_batch_date", value=has_new_batch_date)
    ti.xcom_push(key="has_large_receipts_volume", value=has_large_receipts_volume)
    ti.xcom_push(key="has_quality_drop", value=has_quality_drop)
    ti.xcom_push(key="model_expires_soon", value=model_expires_soon)
    ti.xcom_push(key="hours_until_expiration", value=hours_left)
    ti.xcom_push(key="retrain_reasons", value=retrain_reasons)

    message = (
        f"batch={batch_date}; event={batch_event}; "
        f"receipts={total_receipts:,}; "
        f"current_acc={current_accuracy:.3f}; "
        f"new_batch_date={has_new_batch_date}; "
        f"large_receipts_volume={has_large_receipts_volume}; "
        f"quality_drop={has_quality_drop}; "
        f"expires_soon={model_expires_soon}; "
        f"retrain={should_retrain}; "
        f"reasons={retrain_reasons}"
    )

    print(f"[CT] {message}")
    _append_task_log("check_ct_conditions", message)

    if should_retrain:
        return "train_model"

    return "skip_retraining"


def _get_hours_until_expiration(registry_path: Path) -> float:
    try:
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        prod = [e for e in registry if e.get("stage") == "production"]

        if prod:
            latest = sorted(prod, key=lambda e: e.get("registered_at", ""))[-1]
            registered_at = datetime.fromisoformat(latest["registered_at"])
            model_ttl_hours = 24
            expires_at = registered_at + timedelta(hours=model_ttl_hours)
            return (expires_at - datetime.now()).total_seconds() / 3600
    except Exception:
        pass

    # Если registry ещё нет, не запускаем retraining только из-за TTL.
    # Первый retraining должен происходить из-за новых данных или падения качества.
    return 24.0


def skip_retraining(**context):
    _append_task_log(
        "skip_retraining",
        "Переобучение пропущено — CT-условия не выполнены"
    )
    print("[skip] CT-условия не выполнены. Текущая production-модель остаётся без изменений.")


# ─────────────────────────────────────────────────────────────
# Обучение и оценка
# ─────────────────────────────────────────────────────────────

def load_training_history_until(batch_date: str):
    """Загружает все batch-и из MinIO/S3 до выбранной даты включительно."""
    import pandas as pd

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    keys = hook.list_keys(
        bucket_name=MINIO_BUCKET,
        prefix="incoming/",
    ) or []

    inventory_keys = sorted(
        key for key in keys
        if key.endswith("/inventory.csv")
    )

    frames = []
    used_keys = []

    temp_dir = DATA_DIR / "history_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    for key in inventory_keys:
        parts = key.split("/")
        if len(parts) < 3:
            continue

        current_batch_date = parts[1]

        if current_batch_date <= batch_date:
            local_path = temp_dir / f"{current_batch_date}_inventory.csv"

            obj = hook.get_key(
                key=key,
                bucket_name=MINIO_BUCKET,
            )

            if obj is None:
                continue

            obj.download_file(str(local_path))

            frames.append(pd.read_csv(local_path))
            used_keys.append(f"s3://{MINIO_BUCKET}/{key}")

    if not frames:
        raise FileNotFoundError(
            f"Не найдены MinIO/S3 batch-и до даты {batch_date}"
        )

    history_df = pd.concat(frames, ignore_index=True)

    print(f"[history] Загружено {len(history_df)} строк из {len(used_keys)} batch-ей")
    for key in used_keys:
        print(f"[history] used: {key}")

    return history_df, used_keys


def train_model(**context):
    import pandas as pd
    from inventory_train import train as train_model_fn

    ti = context["ti"]

    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")
    current_batch_path = ti.xcom_pull(task_ids="load_inventory_data", key="batch_path")

    try:
        train_df, used_paths = load_training_history_until(batch_date)
        source_description = f"{len(used_paths)} incoming batch-ей до {batch_date}"
    except Exception as e:
        print(f"[train_model] Не удалось загрузить историю: {e}")
        print("[train_model] Fallback: обучаемся только на текущем batch-е")
        train_df = pd.read_csv(current_batch_path)
        used_paths = [current_batch_path]
        source_description = "текущий batch only"

    metrics = train_model_fn(train_df)

    ti.xcom_push(key="train_metrics", value=metrics)
    ti.xcom_push(key="training_rows", value=len(train_df))
    ti.xcom_push(key="training_batches", value=used_paths)

    _append_task_log(
        "train_model",
        (
            f"source={source_description}; "
            f"rows={len(train_df)}; "
            f"target={metrics.get('target')}; "
            f"acc={metrics.get('accuracy_proxy')}; "
            f"sMAPE={metrics.get('mape_pct')}%"
        ),
    )

    print(f"[train_model] Обучение завершено: {source_description}")
    print(metrics)


def evaluate_model(**context):
    from inventory_evaluate import evaluate

    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="train_model", key="train_metrics")

    evaluated_metrics = evaluate(metrics)

    ti.xcom_push(key="evaluated_metrics", value=evaluated_metrics)

    gate = "✅" if evaluated_metrics["passes_quality_gate"] else "❌"

    _append_task_log(
        "evaluate_model",
        (
            f"{gate} acc={evaluated_metrics['accuracy_proxy']:.3f}; "
            f"sMAPE={evaluated_metrics['mape_pct']}%; "
            f"gate={evaluated_metrics['passes_quality_gate']}"
        ),
    )

    print("[evaluate_model] Оценка модели завершена")
    print(evaluated_metrics)


def decide_deploy(**context):
    """BranchPythonOperator: возвращает register_model или skip_deploy."""
    from inventory_evaluate import compare_with_baseline

    ti = context["ti"]

    evaluated_metrics = ti.xcom_pull(task_ids="evaluate_model", key="evaluated_metrics")

    decision = compare_with_baseline(
        evaluated_metrics,
        REPORTS_DIR / "local_registry.json",
    )

    _append_task_log(
        "compare_with_baseline",
        (
            f"decision={decision}; "
            f"acc={evaluated_metrics.get('accuracy_proxy')}; "
            f"passes_gate={evaluated_metrics.get('passes_quality_gate')}"
        ),
    )

    return decision


def register_model(**context):
    from inventory_registry import register, promote_to_production

    ti = context["ti"]

    evaluated_metrics = ti.xcom_pull(task_ids="evaluate_model", key="evaluated_metrics")
    batch_path = ti.xcom_pull(task_ids="load_inventory_data", key="batch_path")
    batch_date = ti.xcom_pull(task_ids="load_inventory_data", key="batch_date")

    entry = register(
        evaluated_metrics,
        stage="production",
        source_data=batch_path,
        reason=f"CT-цикл: batch={batch_date}, quality gate passed",
    )

    promote_to_production(entry["version"])

    _append_task_log(
        "register_model",
        (
            f"v{entry['version']} → production; "
            f"acc={evaluated_metrics.get('accuracy_proxy')}; "
            f"source={batch_path}"
        ),
    )

    print(f"[registry] Модель v{entry['version']} переведена в production")


def skip_deploy(**context):
    _append_task_log(
        "skip_deploy",
        "Деплой пропущен — новая модель не прошла quality gate или хуже baseline"
    )
    print("[skip] Текущая production-модель остаётся без изменений.")


# ─────────────────────────────────────────────────────────────
# Завершение обработки batch-а
# ─────────────────────────────────────────────────────────────

def mark_batch_processed(**context):
    ti = context["ti"]

    batch_date = ti.xcom_pull(task_ids="select_inventory_batch", key="batch_date")

    if not batch_date:
        raise RuntimeError("batch_date не найден в XCom")

    processed = _read_processed_batches()

    if batch_date not in processed:
        processed.append(batch_date)

    _write_processed_batches(processed)

    _append_task_log(
        "mark_batch_processed",
        f"Batch {batch_date} помечен как processed"
    )

    print(f"[processed] Batch {batch_date} marked as processed")


def _append_task_log(task_id: str, message: str) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "airflow_task_logs.md"

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = (
        "# Лог задач Airflow DAG\n\n"
        "| Время | Задача | Сообщение |\n"
        "|---|---|---|\n"
    )

    row = f"| {ts} | `{task_id}` | {message} |\n"

    if not path.exists():
        path.write_text(header, encoding="utf-8")

    with open(path, "a", encoding="utf-8") as f:
        f.write(row)


# ─────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "mlops-hw9",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="inventory_retrain_pipeline",
    description="CT DAG: live processing of incoming inventory batches (HW9)",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "inventory", "continuous-training", "hw9"],
) as dag:

    sensor = PythonSensor(
        task_id="wait_for_new_inventory_batch",
        python_callable=wait_for_new_inventory_batch,
        poke_interval=10,
        timeout=55,
        mode="reschedule",
        soft_fail=True,
    )

    t_select = PythonOperator(
        task_id="select_inventory_batch",
        python_callable=select_inventory_batch,
    )

    t_load = PythonOperator(
        task_id="load_inventory_data",
        python_callable=load_inventory_data,
    )

    t_validate = PythonOperator(
        task_id="validate_inventory_data",
        python_callable=validate_inventory_data,
    )

    t_detect_event = BranchPythonOperator(
        task_id="detect_batch_event",
        python_callable=detect_batch_event,
    )

    t_shock_alert = PythonOperator(
        task_id="shock_alert",
        python_callable=shock_alert,
    )

    t_normal_event = PythonOperator(
        task_id="normal_batch_event",
        python_callable=normal_batch_event,
    )

    t_check_ct = BranchPythonOperator(
        task_id="check_ct_conditions",
        python_callable=check_ct_conditions,
    )

    t_skip_retraining = PythonOperator(
        task_id="skip_retraining",
        python_callable=skip_retraining,
    )

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    t_evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    t_decide = BranchPythonOperator(
        task_id="compare_with_baseline",
        python_callable=decide_deploy,
    )

    t_register = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
    )

    t_skip_deploy = PythonOperator(
        task_id="skip_deploy",
        python_callable=skip_deploy,
    )

    t_mark_processed = PythonOperator(
        task_id="mark_batch_processed",
        python_callable=mark_batch_processed,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    sensor >> t_select >> t_load >> t_validate >> t_check_ct

    # Отдельная side-branch для визуальной демонстрации shock batch-а.
    # Она не влияет на обучение, а только делает событие видимым в Grid.
    t_load >> t_detect_event
    t_detect_event >> t_shock_alert >> t_finish
    t_detect_event >> t_normal_event >> t_finish

    # Основная CT-логика.
    t_check_ct >> t_train >> t_evaluate >> t_decide
    t_check_ct >> t_skip_retraining >> t_finish

    t_decide >> t_register >> t_mark_processed
    t_decide >> t_skip_deploy >> t_mark_processed

    t_mark_processed >> t_finish