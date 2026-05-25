variable "project_name" {
  description = "Название ML-проекта (используется в манифестах и именах ресурсов)"
  type        = string
  default     = "inventory-stock-ml"
}

variable "environment" {
  description = "Окружение: local / staging / production"
  type        = string
  default     = "local"

  validation {
    condition     = contains(["local", "staging", "production"], var.environment)
    error_message = "environment должен быть: local, staging, production"
  }
}

variable "minio_bucket_name" {
  description = "Имя S3-совместимого бакета для входных батчей данных"
  type        = string
  default     = "inventory-batches"
}

variable "model_registry_path" {
  description = "Локальный путь к файлу реестра моделей"
  type        = string
  default     = "../reports/local_registry.json"
}

variable "airflow_dag_id" {
  description = "Идентификатор DAG Airflow"
  type        = string
  default     = "inventory_retrain_pipeline"
}

variable "manifest_output_dir" {
  description = "Директория для сгенерированных манифестов"
  type        = string
  default     = "../reports/terraform_manifests"
}
