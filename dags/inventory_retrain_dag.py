"""DAG: inventory_retrain_pipeline

Continuous Training DAG для расчёта складских запасов сетевого магазина.

Архитектура: батч-обработка с триггерами CT (Continuous Training).
Переобучение запускается автоматически при выполнении хотя бы одного условия:
  1. В S3/MinIO появились новые данные (≥10M чеков)
  2. accuracy текущей модели < 0.85 (детектирует мониторинг)
  3. Срок действия модели истекает через <1 час

Схема конвейера:
  [FileSensor / S3KeySensor]
       ↓
  check_ct_conditions
       ↓
  load_inventory_data
       ↓
  validate_inventory_data
       ↓
  train_model
       ↓
  evaluate_model
       ↓
  compare_with_baseline (BranchPythonOperator)
       ↓                      ↓
  register_model           skip_deploy
       ↓                      ↓
         [finish]

Переменные окружения (docker-compose.yml):
  DZ9_USE_S3_SENSOR=1     → S3KeySensor (MinIO)
  DZ9_USE_S3_SENSOR=0     → FileSensor (локальный файл, по умолчанию)
"""

import os
import sys
import json
import pickle
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Путь к src/ внутри контейнера (смонтирован как /opt/airflow/src)
sys.path.insert(0, "/opt/airflow/src")

# Константы
RECEIPTS_THRESHOLD = 10_000_000
ACCURACY_THRESHOLD = 0.85
MODEL_EXPIRATION_HOURS = 1

DATA_DIR = Path("/opt/airflow/data")
REPORTS_DIR = Path("/opt/airflow/reports")

# ─────────────────────────────────────────────────────────────
# Сенсор данных (FileSensor или S3KeySensor)
# ─────────────────────────────────────────────────────────────

def _build_sensor(dag):
    use_s3 = os.getenv("DZ9_USE_S3_SENSOR", "0") == "1"

    if use_s3:
        # S3KeySensor — проверяет наличие ключа в MinIO/S3
        from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

        bucket = os.getenv("DZ9_MINIO_BUCKET", "inventory-batches")
        return S3KeySensor(
            task_id="wait_for_inventory_batch",
            bucket_name=bucket,
            bucket_key="incoming/{{ ds }}/inventory.csv",
            aws_conn_id="aws_default",
            timeout=600,
            poke_interval=30,
            dag=dag,
        )
    else:
        # FileSensor — проверяет локальный файл (для быстрого тестирования)
        from airflow.sensors.filesystem import FileSensor

        local_file = str(DATA_DIR / "demo_inventory_batch.csv")
        return FileSensor(
            task_id="wait_for_inventory_batch",
            filepath=local_file,
            timeout=600,
            poke_interval=10,
            dag=dag,
        )


# ─────────────────────────────────────────────────────────────
# CT-условия (Continuous Training Loop)
# ─────────────────────────────────────────────────────────────

def check_ct_conditions(**context):
    """Проверяет три условия запуска CT-цикла.

    1. Достаточно новых данных (≥10M чеков + загружены на S3)
    2. Качество модели упало ниже порога
    3. Срок действия модели истекает через <1 час

    Все условия вычисляются и пишутся в XCom для трассируемости.
    """
    import pandas as pd

    # --- Условие 1: новые данные ---
    use_s3 = os.getenv("DZ9_USE_S3_SENSOR", "0") == "1"
    if use_s3:
        # В production здесь читаем метаданные из MinIO/S3
        total_receipts = 12_500_000
        s3_ready = True
    else:
        data_path = DATA_DIR / "demo_inventory_batch.csv"
        try:
            df = pd.read_csv(data_path)
            total_receipts = len(df) * 300  # синтетическое масштабирование
            s3_ready = True
        except Exception:
            total_receipts = 0
            s3_ready = False

    has_new_data = total_receipts >= RECEIPTS_THRESHOLD and s3_ready

    # --- Условие 2: деградация качества ---
    registry_path = REPORTS_DIR / "local_registry.json"
    current_accuracy = _get_current_accuracy(registry_path)
    has_quality_drop = current_accuracy < ACCURACY_THRESHOLD

    # --- Условие 3: срок модели ---
    hours_left = _get_hours_until_expiration(registry_path)
    model_expires_soon = hours_left <= MODEL_EXPIRATION_HOURS

    ti = context["ti"]
    ti.xcom_push(key="total_receipts", value=total_receipts)
    ti.xcom_push(key="current_accuracy", value=current_accuracy)
    ti.xcom_push(key="hours_until_expiration", value=hours_left)
    ti.xcom_push(key="has_new_data", value=has_new_data)
    ti.xcom_push(key="has_quality_drop", value=has_quality_drop)
    ti.xcom_push(key="model_expires_soon", value=model_expires_soon)

    print(f"[CT] Новые данные: {has_new_data} ({total_receipts:,} чеков)")
    print(f"[CT] Деградация: {has_quality_drop} (accuracy={current_accuracy:.3f})")
    print(f"[CT] Истекает: {model_expires_soon} ({hours_left:.2f}ч)")

    if not (has_new_data or has_quality_drop or model_expires_soon):
        raise RuntimeError(
            "Нет условий для переобучения. DAG остановлен на этапе проверки CT."
        )


def _get_current_accuracy(registry_path: Path) -> float:
    try:
        registry = json.loads(registry_path.read_text())
        prod = [e for e in registry if e.get("stage") == "production"]
        if prod:
            latest = sorted(prod, key=lambda e: e.get("registered_at", ""))[-1]
            return latest.get("metrics", {}).get("accuracy_proxy", 0.0)
    except Exception:
        pass
    return 0.82  # симуляция: текущая модель ниже порога → CT запустится


def _get_hours_until_expiration(registry_path: Path) -> float:
    try:
        registry = json.loads(registry_path.read_text())
        prod = [e for e in registry if e.get("stage") == "production"]
        if prod:
            latest = sorted(prod, key=lambda e: e.get("registered_at", ""))[-1]
            registered_at = datetime.fromisoformat(latest["registered_at"])
            model_ttl_hours = 24  # модель действительна 24 часа
            expires_at = registered_at + timedelta(hours=model_ttl_hours)
            return (expires_at - datetime.now()).total_seconds() / 3600
    except Exception:
        pass
    return 0.5  # симуляция: 30 минут до истечения


# ─────────────────────────────────────────────────────────────
# Основные шаги ML-конвейера
# ─────────────────────────────────────────────────────────────

def load_inventory_data(**context):
    from inventory_data import load_batch, log_sensor_event

    use_s3 = os.getenv("DZ9_USE_S3_SENSOR", "0") == "1"
    if use_s3:
        # В production: загружаем файл из MinIO
        endpoint = os.getenv("DZ9_MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("DZ9_MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("DZ9_MINIO_SECRET_KEY", "minioadmin")
        bucket = os.getenv("DZ9_MINIO_BUCKET", "inventory-batches")

        try:
            import boto3
            from botocore.client import Config

            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4"),
            )
            ds = context["ds"]
            key = f"incoming/{ds}/inventory.csv"
            out_path = DATA_DIR / "current_inventory_batch.csv"
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(out_path))
            print(f"[load] Скачан {key} → {out_path}")
            source_path = str(out_path)
        except Exception as e:
            print(f"[load] Ошибка S3: {e}. Используем локальный файл.")
            source_path = str(DATA_DIR / "demo_inventory_batch.csv")
    else:
        source_path = str(DATA_DIR / "demo_inventory_batch.csv")

    import pandas as pd
    df = pd.read_csv(source_path)
    n_receipts = len(df) * 300
    log_sensor_event(source_path, len(df), n_receipts)
    context["ti"].xcom_push(key="batch_path", value=source_path)
    context["ti"].xcom_push(key="n_rows", value=len(df))
    print(f"[load] Загружено {len(df)} строк из {source_path}")


def validate_inventory_data(**context):
    import pandas as pd
    from inventory_validation import validate_and_report

    batch_path = context["ti"].xcom_pull(task_ids="load_inventory_data", key="batch_path")
    df = pd.read_csv(batch_path)
    validate_and_report(df)
    _append_task_log("validate_inventory_data", "✅ Данные прошли валидацию")


def train_model(**context):
    import pandas as pd
    from inventory_train import train as train_model_fn

    batch_path = context["ti"].xcom_pull(task_ids="load_inventory_data", key="batch_path")
    df = pd.read_csv(batch_path)
    metrics = train_model_fn(df)
    context["ti"].xcom_push(key="train_metrics", value=metrics)
    _append_task_log("train_model", f"RMSE={metrics['rmse']}, MAPE={metrics['mape_pct']}%")


def evaluate_model(**context):
    from inventory_evaluate import evaluate

    metrics = context["ti"].xcom_pull(task_ids="train_model", key="train_metrics")
    metrics = evaluate(metrics)
    context["ti"].xcom_push(key="eval_metrics", value=metrics)
    gate = "✅" if metrics["passes_quality_gate"] else "❌"
    _append_task_log(
        "evaluate_model",
        f"{gate} acc={metrics['accuracy_proxy']:.3f} gate={metrics['passes_quality_gate']}",
    )


def decide_deploy(**context):
    """BranchPythonOperator: возвращает имя следующего таска."""
    from inventory_evaluate import compare_with_baseline

    metrics = context["ti"].xcom_pull(task_ids="evaluate_model", key="eval_metrics")
    decision = compare_with_baseline(metrics, REPORTS_DIR / "local_registry.json")
    _append_task_log("decide_deploy", f"Ветка: {decision}")
    return decision


def register_model(**context):
    from inventory_registry import register, promote_to_production

    metrics = context["ti"].xcom_pull(task_ids="evaluate_model", key="eval_metrics")
    batch_path = context["ti"].xcom_pull(task_ids="load_inventory_data", key="batch_path")
    entry = register(
        metrics,
        stage="production",
        source_data=batch_path,
        reason="CT-цикл: автоматическое переобучение (quality gate пройден)",
    )
    promote_to_production(entry["version"])
    _append_task_log("register_model", f"v{entry['version']} → production")


def skip_deploy(**context):
    _append_task_log("skip_deploy", "Деплой пропущен — модель не прошла quality gate")
    print("[skip] Текущая production-модель остаётся без изменений.")


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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="inventory_retrain_pipeline",
    description="CT DAG: переобучение модели складских запасов (HW9)",
    start_date=datetime(2026, 5, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "inventory", "continuous-training", "hw9"],
) as dag:

    sensor = _build_sensor(dag)

    t_check_ct = PythonOperator(
        task_id="check_ct_conditions",
        python_callable=check_ct_conditions,
    )

    t_load = PythonOperator(
        task_id="load_inventory_data",
        python_callable=load_inventory_data,
    )

    t_validate = PythonOperator(
        task_id="validate_inventory_data",
        python_callable=validate_inventory_data,
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

    t_skip = PythonOperator(
        task_id="skip_deploy",
        python_callable=skip_deploy,
    )

    t_finish = EmptyOperator(
        task_id="finish",
        trigger_rule="none_failed_min_one_success",
    )

    # Граф зависимостей
    sensor >> t_check_ct >> t_load >> t_validate >> t_train >> t_evaluate >> t_decide
    t_decide >> t_register >> t_finish
    t_decide >> t_skip >> t_finish
