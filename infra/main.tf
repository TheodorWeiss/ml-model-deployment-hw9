# =============================================================================
# HW9 Infrastructure as Code
#
# Архитектура батч-обучения (выбор из Задания 2):
#   - объектное хранилище (MinIO / S3) для батчей данных;
#   - Airflow для оркестрации пайплайна;
#   - локальный реестр моделей (аналог MLflow Model Registry).
#
# Провайдер: local (без облачных credentials).
# В production заменить на:
#   - yandex_storage_bucket  (Yandex Cloud)
#   - aws_s3_bucket           (AWS)
#   - google_storage_bucket   (GCP)
#
# Деинсталляция: terraform destroy
# =============================================================================

resource "random_id" "run_id" {
  byte_length = 4
}

# --- Директория для манифестов ---
resource "local_file" "manifest_dir_placeholder" {
  filename = "${var.manifest_output_dir}/.gitkeep"
  content  = ""
}

# --- Манифест объектного хранилища (S3 / MinIO) ---
resource "local_file" "storage_manifest" {
  filename = "${var.manifest_output_dir}/storage.json"
  content = jsonencode({
    resource_type  = "object_storage_bucket"
    cloud_provider = "local_minio"
    bucket_name    = var.minio_bucket_name
    environment    = var.environment
    versioning     = true
    lifecycle_rules = [{
      id         = "expire-old-batches"
      enabled    = true
      expiration = { days = 90 }
    }]
    comment = "В Yandex Cloud: yandex_storage_bucket; в AWS: aws_s3_bucket"
    run_id  = random_id.run_id.hex
  })
}

# --- Манифест Airflow DAG ---
resource "local_file" "airflow_dag_manifest" {
  filename = "${var.manifest_output_dir}/airflow_dag.json"
  content = jsonencode({
    resource_type = "workflow_dag"
    orchestrator  = "apache_airflow"
    dag_id        = var.airflow_dag_id
    schedule      = "@hourly"
    environment   = var.environment
    docker_image  = "apache/airflow:2.8.4"
    dag_file      = "../dags/inventory_retrain_dag.py"
    comment       = "В Yandex Managed Airflow или MWAA: настройка через Terraform provider"
    run_id        = random_id.run_id.hex
  })
}

# --- Манифест ML Pipeline / Model Training Infrastructure ---
resource "local_file" "ml_pipeline_manifest" {
  filename = "${var.manifest_output_dir}/ml_pipeline.json"
  content = jsonencode({
    resource_type  = "ml_pipeline"
    project        = var.project_name
    environment    = var.environment
    training_image = "python:3.12-slim"
    framework      = "scikit-learn"
    model_type     = "LinearRegression"
    registry_path  = var.model_registry_path
    artifact_store = "local_file_system"
    comment        = "В production: Vertex AI / SageMaker / YandexML pipeline"
    run_id         = random_id.run_id.hex
  })
}

# --- Манифест реестра моделей ---
resource "local_file" "model_registry_manifest" {
  filename = "${var.manifest_output_dir}/model_registry.json"
  content = jsonencode({
    resource_type = "model_registry"
    backend       = "local_json"
    registry_file = var.model_registry_path
    stages        = ["production_candidate", "production", "archived"]
    comment       = "В production заменить на MLflow Server или Yandex DataSphere"
    run_id        = random_id.run_id.hex
  })
}

# --- Манифест мониторинга ---
resource "local_file" "monitoring_manifest" {
  filename = "${var.manifest_output_dir}/monitoring.json"
  content = jsonencode({
    resource_type      = "monitoring"
    sli_accuracy_proxy = ">= 0.85"
    sli_smape          = "<= 50%"
    sli_dag_success    = ">= 99%"
    sli_latency_p95    = "<= 300ms"
    alert_channel      = "email / slack"
    comment            = "В production: Prometheus + Grafana или Yandex Monitoring"
    run_id             = random_id.run_id.hex
  })
}
