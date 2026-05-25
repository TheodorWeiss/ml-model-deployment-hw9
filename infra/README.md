# Инфраструктура ML-системы (IaC — Terraform)

## Почему используется локальный провайдер

Задание не требует реальных облачных ресурсов и credentials.  
Локальный провайдер `hashicorp/local` генерирует **JSON-манифесты** описывающие:
- объектное хранилище (MinIO / S3);
- оркестратор Airflow;
- ML-пайплайн и реестр моделей;
- мониторинг.

В реальном проекте те же `.tf`-файлы (с минимальными изменениями) применяются с:
- `yandex_storage_bucket` (Yandex Cloud)
- `aws_s3_bucket` (AWS)
- `google_storage_bucket` (GCP)

## Ресурсы

| Ресурс (local) | В production |
|---|---|
| `local_file.storage_manifest` | S3 / MinIO bucket для батчей данных |
| `local_file.airflow_dag_manifest` | Managed Airflow (MWAA / Yandex Managed Airflow) |
| `local_file.ml_pipeline_manifest` | ML Training Infrastructure |
| `local_file.model_registry_manifest` | MLflow Server / Model Registry |
| `local_file.monitoring_manifest` | Prometheus + Grafana / Yandex Monitoring |

## Быстрый старт

```bash
cd infra
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

Сгенерированные манифесты появятся в `reports/terraform_manifests/`.

## Деинсталляция инфраструктуры

```bash
cd infra
terraform destroy
```

Terraform удалит все локальные манифесты.  
В production эта же команда удалит облачные ресурсы, предотвращая утечки стоимости.

## Интеграция с CI/CD

Файл `.github/workflows/dz9-checks.yml` выполняет:
```
terraform fmt -check
terraform init
terraform validate
terraform plan
```

Это гарантирует, что IaC-конфигурация корректна при каждом пуше в репозиторий.
