output "project_name" {
  description = "Название проекта"
  value       = var.project_name
}

output "environment" {
  description = "Окружение"
  value       = var.environment
}

output "storage_manifest_path" {
  description = "Путь к манифесту объектного хранилища"
  value       = local_file.storage_manifest.filename
}

output "airflow_dag_manifest_path" {
  description = "Путь к манифесту Airflow DAG"
  value       = local_file.airflow_dag_manifest.filename
}

output "ml_pipeline_manifest_path" {
  description = "Путь к манифесту ML-пайплайна"
  value       = local_file.ml_pipeline_manifest.filename
}

output "model_registry_manifest_path" {
  description = "Путь к манифесту реестра моделей"
  value       = local_file.model_registry_manifest.filename
}

output "monitoring_manifest_path" {
  description = "Путь к манифесту мониторинга"
  value       = local_file.monitoring_manifest.filename
}

output "destroy_note" {
  description = "Команда деинсталляции инфраструктуры"
  value       = "Для удаления всей инфраструктуры: cd infra && terraform destroy"
}
