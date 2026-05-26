# Задание 4. Управление рисками через SLI/SLO

## 1. Контекст системы

В проекте реализована учебная ML-система для прогнозирования складских остатков в розничной сети.

Целевая переменная модели — `stock_qty_next_day`, то есть прогноз остатка товара на следующий день. Такой прогноз может использоваться как вспомогательный сигнал для решений о пополнении запасов, предотвращении out-of-stock и снижении overstock.

Система построена как Continuous Training pipeline:

- daily batch-и публикуются во входную область MinIO/S3;
- Airflow обнаруживает новый batch и запускает DAG;
- batch загружается и проходит валидацию;
- текущая production-модель проверяется на новом batch-е;
- при выполнении CT-условий запускается retraining;
- новая модель оценивается через quality gate;
- candidate model сравнивается с baseline;
- если модель безопасна, она регистрируется в локальном model registry;
- если модель хуже baseline или не проходит quality gate, deployment пропускается.

Главный принцип риск-менеджмента: **retraining и deployment разделены**.  
Система может автоматически переобучить модель, но не обязана автоматически продвигать её в production.

---

## 2. Основные риски системы

## 2.1 Бизнес-риски

| Риск | Почему это важно |
|---|---|
| Out-of-stock | Если товар закончился на складе/в магазине, клиент не может его купить, продажи теряются. |
| Overstock | Избыточные запасы замораживают деньги, занимают место и могут привести к списаниям. |
| Ошибочное пополнение | Неверный прогноз может привести к слишком большому или слишком маленькому заказу. |
| Устаревшая модель | Поведение продаж меняется, и старая модель может перестать быть полезной. |
| Автоматический deployment плохой модели | Новая модель может быть хуже текущей и ухудшить бизнес-решения. |

## 2.2 Риски данных

| Риск | Почему это важно |
|---|---|
| Нет нового batch-а | Модель и мониторинг работают на устаревших данных. |
| Дубликат batch-а | Один и тот же день может быть обработан повторно. |
| Ошибка схемы данных | В batch-е может не быть нужных колонок. |
| Пропуски в данных | Модель может обучиться на неполных или некорректных данных. |
| Некорректные значения | Отрицательные продажи, остатки или цены искажают обучение. |
| Shock / drift | Распределение данных резко меняется, и качество модели падает. |

## 2.3 Риски модели и кода

| Риск | Почему это важно |
|---|---|
| Падение качества текущей модели | Production-модель больше не даёт надёжный прогноз. |
| Candidate model хуже baseline | Новая модель обучилась, но хуже текущей production-модели. |
| Ошибка quality gate | Плохая модель может быть ошибочно зарегистрирована. |
| Ошибка DAG-ветвления | Airflow может запустить неправильную ветку: retrain/skip/deploy. |
| Batch помечен processed слишком рано | Ошибка может скрыть неуспешную обработку batch-а. |

## 2.4 Инфраструктурные риски

| Риск | Почему это важно |
|---|---|
| MinIO/S3 недоступен | Airflow не может найти или скачать новые batch-и. |
| Airflow scheduler не работает | DAG-run не создаётся, CT-процесс останавливается. |
| Airflow webserver недоступен | Невозможно удобно контролировать pipeline. |
| Postgres недоступен | Airflow не может хранить metadata и состояние задач. |
| Ошибка GitHub Actions | В main может попасть сломанный код или IaC. |
| Ошибка Terraform validation/plan | Инфраструктурное описание становится невоспроизводимым. |

---

## 3. SLI/SLO на трёх уровнях

## 3.1 Уровень 1 — бизнес

| SLI | Нормальный SLO | Критический порог | Действие |
|---|---:|---:|---|
| Свежесть обработанного batch-а | Новый daily batch обработан в ожидаемый интервал | Нет нового обработанного batch-а за 2 интервала | Проверить producer, MinIO и Airflow sensor |
| Доля SKU-store прогнозов, доступных для использования | ≥ 95% | < 80% | Проверить данные, DAG и модель |
| Proxy-точность прогноза остатков (`accuracy_proxy`) | ≥ 0.85 | < 0.80 | Запустить retraining и проверить причину деградации |
| Out-of-stock risk proxy | < 5% SKU-store пар с критически низким прогнозом остатка | > 15% | Ручная проверка пополнения и бизнес-эскалация |
| Overstock risk proxy | < 10% SKU-store пар с чрезмерным прогнозом остатка | > 25% | Пересмотреть пополнение, акции или параметры модели |

**Комментарий.**  
В учебном проекте out-of-stock и overstock не реализованы как отдельные автоматические метрики, но они являются бизнес-интерпретацией ошибки прогноза `stock_qty_next_day`. Поэтому их можно описать как production extension.

---

## 3.2 Уровень 2 — данные, модель и код

| SLI | Нормальный SLO | Критический порог | Действие | Где реализовано |
|---|---:|---:|---|---|
| Наличие обязательных колонок | 100% required columns present | Отсутствует хотя бы одна required column | Остановить pipeline до training | `validate_inventory_data` |
| Минимальный размер batch-а | ≥ 100 строк | < 100 строк | Заблокировать обучение | `inventory_validation.py` |
| Доля пропусков в колонках | ≤ 5% | > 5% | Заблокировать или расследовать batch | `inventory_validation.py` |
| Некорректные отрицательные значения | 0 критических ошибок | Есть отрицательные значения в sales/stock/price | Заблокировать batch | `inventory_validation.py` |
| Качество текущей модели на новом batch-е | `current_accuracy >= 0.85` | `< 0.85` | Trigger retraining | `check_ct_conditions` |
| Quality gate candidate model | `accuracy_proxy >= 0.85` и `MAPE <= 35%` | Не выполнено хотя бы одно условие | `skip_deploy` | `inventory_evaluate.py` |
| Candidate model vs baseline | Candidate accuracy ≥ baseline accuracy | Candidate хуже baseline | Не регистрировать модель | `compare_with_baseline` |
| Shock/drift visibility | Batch event normal или shock определён | Shock без расследования | Логировать shock и проверить качество | `detect_batch_event`, `shock_alert` |
| Успешность DAG-ветвления | Одна ветка register/skip_deploy green, другая skipped | Обе ветки failed/skipped unexpectedly | Проверить BranchPythonOperator и trigger rules | Airflow Grid |

---

## 3.3 Уровень 3 — инфраструктура

| SLI | Нормальный SLO | Критический порог | Действие | Где проверяется |
|---|---:|---:|---|---|
| Airflow DAG run success | ≥ 99% в production / успешно в demo | Повторяющиеся failed DAG-runs | Смотреть Airflow logs, исправить DAG | Airflow UI |
| Airflow scheduler availability | Scheduler создаёт DAG-runs по расписанию | Нет DAG-runs за 2 интервала | Restart scheduler container | Docker Compose / Airflow |
| Airflow webserver availability | UI доступен во время проверки | UI недоступен | Restart webserver container | Docker Compose |
| MinIO/S3 availability | Bucket доступен, batch-и читаются | Airflow не может list/download objects | Проверить MinIO, bucket, credentials | MinIO Console / DAG logs |
| Postgres availability | Airflow metadata DB healthy | Airflow не стартует или теряет metadata | Проверить Postgres container/volume | Docker Compose |
| Disk/storage usage | < 70% usage | > 90% usage | Очистить старые logs/batches, проверить volumes | Docker / OS |
| GitHub Actions success | Все checks green | Python smoke test или Terraform validation failed | Block merge, исправить код/IaC | GitHub Actions |
| Terraform IaC validation | `terraform validate` и `plan` successful | validate/plan failed | Исправить IaC до merge | GitHub Actions / Terraform |

---

## 4. Критические SLO

Критическими считаются нарушения, при которых deployment должен быть остановлен или требуется ручная проверка.

| Critical condition | Последствие | Действие |
|---|---|---|
| `current_accuracy < 0.85` | Текущая модель деградировала | Запустить retraining |
| Candidate model не проходит quality gate | Новая модель небезопасна | `skip_deploy`, оставить текущую production-модель |
| Candidate model хуже baseline | Deployment ухудшит качество | `skip_deploy` |
| В batch-е отсутствуют required columns | Training некорректен | Остановить pipeline |
| Missing rate > 5% | Данные ненадёжны | Остановить или расследовать batch |
| Повторяющиеся Airflow task failures | CT-процесс нестабилен | Ручная диагностика |
| MinIO/S3 недоступен | Нет доступа к batch-ам | Остановить batch processing |
| Postgres недоступен | Airflow metadata broken | Восстановить DB до продолжения |
| GitHub Actions failed | Код/IaC не прошли проверку | Не merge-ить изменения |
| Terraform validation/plan failed | IaC невоспроизводим | Исправить IaC |

---

## 5. Mapping SLI/SLO к артефактам проекта

| Область | Артефакт |
|---|---|
| Batch detection | `wait_for_new_inventory_batch`, Airflow sensor logs |
| Batch loading | `load_inventory_data` |
| Data validation | `validate_inventory_data`, `reports/data_validation.md` |
| CT trigger decision | `check_ct_conditions`, `reports/airflow_task_logs.md` |
| Shock detection | `detect_batch_event`, `shock_alert` |
| Model training | `train_model`, `src/inventory_train.py` |
| Model evaluation | `evaluate_model`, `reports/model_metrics.md` |
| Baseline comparison | `compare_with_baseline`, `reports/airflow_compare_log.md` |
| Model registry | `register_model`, `reports/local_registry.json` |
| Skip unsafe deployment | `skip_deploy` |
| Processed batch tracking | `mark_batch_processed`, `reports/processed_batches.json` |
| Object storage | MinIO bucket `inventory-batches` |
| Orchestration | Airflow Grid / Graph |
| Runtime infrastructure | `docker-compose.yml` |
| IaC | `infra/main.tf`, Terraform plan/apply/destroy |
| CI/CD | `.github/workflows/dz9-checks.yml` |

---

## 6. Реакция системы на риски

| Ситуация | Автоматическая реакция | Ручная реакция |
|---|---|---|
| Новый valid batch | Запуск CT-проверок и retraining | Не требуется |
| Нет нового batch-а | Sensor ждёт / DAG-run soft-fail | Проверить producer, если ожидался batch |
| Schema validation failed | Pipeline останавливается до training | Исправить источник данных |
| Quality drop текущей модели | Trigger retraining | Проверить причину падения качества |
| Shock batch | Ветка `shock_alert` становится green, событие логируется | Проверить, бизнес-шок это или ошибка данных |
| Candidate model passed gate and ≥ baseline | `register_model` | Контроль registry |
| Candidate model failed gate or < baseline | `skip_deploy` | Анализ причин, текущая модель остаётся production |
| Airflow task failed | DAG-run failed | Проверить task logs |
| GitHub Actions failed | Merge должен быть заблокирован | Исправить код/IaC |

---

## 7. Что можно добавить в production-версии

В рамках учебного проекта часть SLI/SLO фиксируется через Airflow UI, markdown-отчёты и локальные JSON-файлы. В production-версии эту схему можно расширить:

- Prometheus metrics для Airflow task status, DAG duration и service health;
- Grafana dashboard для model quality, batch freshness, drift/shock events;
- MinIO metrics для object storage availability;
- Postgres health metrics;
- alerting rules для critical SLO;
- log aggregation для Airflow task logs;
- автоматический отчёт по out-of-stock / overstock risk proxy.

Примеры production alerts:

- `current_accuracy < 0.85`;
- candidate model failed quality gate;
- two consecutive failed DAG-runs;
- no new batch for two expected intervals;
- MinIO bucket unavailable;
- disk usage > 90%;
- GitHub Actions failed on `main`.

---

## 8. Вывод

В проекте управление рисками реализовано на трёх уровнях:

1. **Бизнес-уровень**  
   Контроль качества прогноза, свежести batch-ей, риска out-of-stock и overstock.

2. **Уровень данных, модели и кода**  
   Валидация схемы данных, контроль пропусков, quality gate, сравнение candidate model с baseline, shock detection.

3. **Инфраструктурный уровень**  
   Контроль Airflow, MinIO, Postgres, Terraform и GitHub Actions.

Ключевая идея: система не делает автоматический deployment каждой новой модели.  
Новый batch может запустить retraining, но candidate model попадёт в production только если она проходит quality gate и не хуже baseline.

Таким образом, Continuous Training pipeline используется не только для переобучения модели, но и как механизм управления рисками ML-системы.