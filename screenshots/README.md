# Чеклист скриншотов для сдачи HW9

Сделайте скриншоты и сохраните их в эту папку.

## 1. Структура репозитория
- [ ] Скриншот дерева файлов (VS Code Explorer или `tree` в терминале)

## 2. Airflow DAG
- [ ] Список DAG-ов в Airflow UI (http://localhost:8080)
- [ ] Graph View DAG `inventory_retrain_pipeline` (видны все задачи и ветвление)
- [ ] Успешный запуск DAG (все задачи зелёные)
- [ ] Логи задачи `compare_with_baseline` — показать ветку `register_model` или `skip_deploy`
- [ ] Логи задачи `register_model` (если запущена)

## 3. MinIO (S3-совместимое хранилище)
- [ ] Консоль MinIO (http://localhost:9001) — бакет `inventory-batches`
- [ ] Загруженный файл `demo_inventory_batch.csv` или `inventory.csv` в бакете

## 4. Terraform (IaC)
- [ ] Вывод `terraform validate` (должно быть: `Success!`)
- [ ] Вывод `terraform plan` (список ресурсов к созданию)
- [ ] Вывод `terraform plan -destroy` (список ресурсов к удалению)

## 5. Сгенерированные отчёты
- [ ] Файл `reports/model_metrics.md` (метрики модели, quality gate)
- [ ] Файл `reports/local_registry.json` (запись о зарегистрированной модели)
- [ ] Файл `reports/mdd_test_result.md` (результат статистического теста)
- [ ] График `reports/mdd_latency_distribution.png`

## 6. CI/CD (GitHub Actions)
- [ ] Успешно прошедший workflow `HW9 CI Checks` в разделе Actions

---
*Скриншоты нужно сделать вручную после запуска Docker Compose и Terraform.*
