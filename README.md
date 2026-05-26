# HW9. Проектирование ML-системы расчёта складских запасов

**Модуль 9: CI/CD, оркестрация, IaC и ML System Design**

Airflow управляет ML-процессом, MinIO имитирует S3, Terraform описывает инфраструктуру декларативно, GitHub Actions проверяет код и IaC, локальный реестр хранит метаданные моделей, SLI/SLO описывают риски, MDD обосновывает архитектурные решения.

---

## Соответствие критериям оценки

| Критерий | Где смотреть | Артефакт |
|---|---|---|
| **Задание 1** Архитектуры ML-конвейеров | Ноутбук HW9 (Task 1, ответы студента) | Ячейка с ответами |
| **Задание 2** Airflow DAG (расчёт остатков) | `dags/inventory_retrain_dag.py` | DAG `inventory_retrain_pipeline` |
| **Задание 2** Отслеживание файлов на S3 | `dags/inventory_retrain_dag.py` → `_build_sensor()` | `FileSensor` / `S3KeySensor` (env `DZ9_USE_S3_SENSOR`) |
| **Задание 2** CT-условия (данные / accuracy / TTL) | `dags/inventory_retrain_dag.py` → `check_ct_conditions()` | 3 условия + XCom |
| **Задание 3** IaC (Terraform) | `infra/` | `main.tf`, `providers.tf`, `variables.tf`, `outputs.tf` |
| **Задание 3** Деинсталляция инфраструктуры | `infra/README.md` | `terraform destroy` |
| **Задание 3** CI/CD-проверки IaC | `.github/workflows/dz9-checks.yml` | `terraform fmt`, `init`, `validate`, `plan`, `plan -destroy` |
| **Задание 4** SLI/SLO (3 уровня) | Раздел «Риски и SLI/SLO» ниже | Таблица с 3 уровнями |
| **Задание 5** MDD (системная метрика) | `src/mdd_latency_test.py` | p95 latency, Mann–Whitney U |
| **Задание 5** ADR | `adr/0001-latency-mdd-decision.md` | Статус, контекст, решение |
| **Задание 5** Отчёт теста | `reports/mdd_test_result.md` | p-value, δp95, решение |
| **Задание 5** Визуализация статистического теста | [`reports/mdd_latency_distribution.png`](reports/mdd_latency_distribution.png) | Распределение latency, boxplot, SLO=300мс, critical=1000мс |

---

## Структура репозитория

```
ml-model-deployment-hw9/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .gitignore
├── HW9_Design_Вайс_ФС.ipynb
│
├── dags/
│   └── inventory_retrain_dag.py   ← Airflow DAG (CT loop)
│
├── src/
│   ├── inventory_data.py          ← генерация и загрузка данных
│   ├── inventory_validation.py    ← валидация схемы батча
│   ├── inventory_train.py         ← обучение LinearRegression
│   ├── inventory_evaluate.py      ← quality gate + compare with baseline
│   ├── inventory_registry.py      ← локальный реестр моделей (JSON)
│   └── mdd_latency_test.py        ← MDD: тест задержки (Mann–Whitney)
│
├── data/
│   ├── demo_inventory_batch.csv   ← синтетический батч (4500 строк)
│   ├── inventory_test.csv         ← тестовая выборка
│   ├── reference_latency.csv      ← референсные задержки (~264 мс p95)
│   └── new_latency.csv            ← новые задержки (~1376 мс p95, деградация)
│
├── infra/
│   ├── README.md                  ← описание IaC-подхода
│   ├── providers.tf               ← hashicorp/local + hashicorp/random
│   ├── variables.tf               ← переменные конфигурации
│   ├── main.tf                    ← ресурсы (манифесты компонентов)
│   └── outputs.tf                 ← выходные значения Terraform
│
├── reports/                       ← авто-генерируемые отчёты
│   ├── data_validation.md
│   ├── model_metrics.md
│   ├── local_registry.json
│   ├── airflow_sensor_log.md
│   ├── airflow_task_logs.md
│   ├── airflow_compare_log.md
│   ├── airflow_registry_log.md
│   ├── mdd_test_result.md
│   ├── mdd_latency_distribution.png
│   ├── terraform_plan.txt
│   └── terraform_destroy_plan.txt
│
├── adr/
│   └── 0001-latency-mdd-decision.md  ← ADR по результатам MDD
│
├── screenshots/
│   └── README.md                  ← чеклист скриншотов
│
└── .github/
    └── workflows/
        └── dz9-checks.yml         ← CI/CD: Python + Terraform checks
```

---

## Архитектура системы

```
[S3 / MinIO] ──FileSensor/S3KeySensor──► [check_ct_conditions]
                                                  │
                          ┌───────────────────────┤
                          ▼                       ▼
                   (≥10M чеков              (accuracy<0.85
                    на S3)                   или TTL<1ч)
                          └───────────────────────┘
                                          │
                                ┌─────────▼──────────┐
                                │  load_inventory     │
                                │  validate_data      │
                                │  train_model        │
                                │  evaluate_model     │
                                └─────────┬──────────┘
                                          │
                               ┌──────────▼──────────┐
                               │ compare_with_baseline│
                               │ (BranchOperator)     │
                               └────┬──────────┬─────┘
                                    │          │
                             register_model  skip_deploy
                                    │          │
                               [local_registry.json]
                                    └────┬─────┘
                                       finish
```

**Компоненты** (только необходимые для батч-архитектуры, без Feature Store и Serving Layer):
- **Source Code Repository** — GitHub
- **CI/CD Component** — GitHub Actions (`dz9-checks.yml`)
- **Workflow Orchestration** — Apache Airflow (Docker Compose)
- **Object Storage** — MinIO (S3-совместимый)
- **Model Registry** — локальный JSON реестр (`reports/local_registry.json`)
- **Monitoring** — SLI/SLO (описание ниже)

---

## Задание 2: Continuous Training Loop

DAG реализует три CT-условия из задания:

| Условие | Реализация в `check_ct_conditions()` |
|---|---|
| ≥10M чеков загружены на S3 | `total_receipts >= 10_000_000 and s3_ready` |
| `accuracy < 0.85` (деградация) | `current_accuracy < ACCURACY_THRESHOLD` |
| Срок модели истекает через <1ч | `hours_until_expiration <= 1.0` |

Если ни одно условие не выполнено → DAG прерывается на `check_ct_conditions`.

### Отслеживание файлов на S3

Переключается переменной окружения:

| `DZ9_USE_S3_SENSOR` | Сенсор | Применение |
|---|---|---|
| `0` (по умолчанию) | `FileSensor` | Локальное тестирование |
| `1` | `S3KeySensor` (MinIO) | Production / Docker demo |

---

## Задание 3: IaC (Terraform)

Terraform описывает инфраструктуру декларативно через `local_file` манифесты.
Реальные облачные ресурсы не нужны — достаточно `terraform validate` и `plan`.

```bash
cd infra
terraform init
terraform fmt -check
terraform validate     # Success!
terraform plan         # 7 ресурсов к созданию
terraform plan -destroy  # 7 ресурсов к удалению (деинсталляция)
```

**Деинсталляция:** `terraform destroy` — удаляет все созданные манифесты/ресурсы.

---

## Задание 4: Управление рисками и SLI/SLO

Для ML-системы прогнозирования складских остатков описана схема управления рисками через SLI/SLO на трёх уровнях:

1. **Бизнес-уровень** — свежесть batch-ей, качество прогноза остатков, out-of-stock и overstock risk proxy.
2. **Уровень данных, модели и кода** — schema validation, missing values, quality gate, `accuracy_proxy`, MAPE, сравнение candidate model с baseline, shock detection.
3. **Инфраструктурный уровень** — Airflow, MinIO/S3, Postgres, Docker Compose, Terraform и GitHub Actions.

Полный отчёт находится здесь:

[`reports/risk_management_sli_slo.md`](reports/risk_management_sli_slo.md)

Критические SLO-нарушения блокируют deployment или требуют ручной проверки: падение качества модели ниже порога, ошибка валидации данных, candidate model хуже baseline, недоступность MinIO/Postgres/Airflow или падение CI/CD-проверок.

---

## Задание 5: Metrics Driven Development

**Метрика:** p95 задержки inventory forecast (мс) — системная, не только модельная.

**Результат статистического теста:**

| | Референс | Новые данные |
|---|---|---|
| N | 200 | 200 |
| p95 | ~264 мс | ~1376 мс |
| Тест | Mann–Whitney U | |
| p-value | < 0.001 | |
| Значимо? | Да (p < 0.05) | |
| Превышен SLO (300мс)? | Да | |

**Решение: ДЕЙСТВОВАТЬ** — добавить кеш, предрасчёт признаков, удержать старую модель.

Детали: [`reports/mdd_test_result.md`](reports/mdd_test_result.md)  
ADR: [`adr/0001-latency-mdd-decision.md`](adr/0001-latency-mdd-decision.md)

---

## Запуск (Windows PowerShell)

### 1. Установка зависимостей Python

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Smoke test пайплайна

```powershell
python -c "
import sys; sys.path.insert(0, 'src')
from inventory_data import generate_inventory_batch
from inventory_train import train
from inventory_evaluate import evaluate, compare_with_baseline
df = generate_inventory_batch(n_skus=5, n_days=15, n_stores=2)
m = train(df)
m = evaluate(m)
print('Decision:', compare_with_baseline(m))
"
```

### 3. MDD-тест задержки

```powershell
python src/mdd_latency_test.py
# Результат: reports/mdd_test_result.md, reports/mdd_latency_distribution.png
```

### 4. Docker Compose (Airflow + MinIO)

```powershell
docker compose up -d
# Airflow UI: http://localhost:8080  (admin / admin)
# MinIO Console: http://localhost:9001  (minioadmin / minioadmin)
```

Загрузить демо-батч в MinIO (если DZ9_USE_S3_SENSOR=1):

```powershell
# Через MinIO Console или mc CLI:
docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin
docker run --rm --network host -v "${PWD}/data:/data" minio/mc cp /data/demo_inventory_batch.csv local/inventory-batches/incoming/2026-05-24/inventory.csv
```

Запустить DAG вручную в Airflow UI: меню DAGs → `inventory_retrain_pipeline` → кнопка ▶

### 5. Terraform

```powershell
cd infra
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform plan -destroy
```

---

## Запуск (Linux / WSL)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/mdd_latency_test.py
docker compose up -d
cd infra && terraform init && terraform validate && terraform plan
```

---

## Примечания

- Реальные AWS/Yandex credentials **не требуются** — MinIO локальный, Terraform использует `local` провайдер.
- Модель (LinearRegression) намеренно простая: задание оценивает **оркестрацию и системное проектирование**, а не качество модели.
- Feature Store и Model Serving **намеренно не включены**: задание требует только необходимые компоненты выбранной архитектуры (батч).
- CI/CD роль: **проверка кода и IaC**, не запуск обучения (это роль Airflow).
