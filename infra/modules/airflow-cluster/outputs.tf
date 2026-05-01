output "cluster_id" {
  description = "Managed Airflow cluster ID"
  value       = yandex_airflow_cluster.airflow_cluster.id
}

output "cluster_name" {
  description = "Managed Airflow cluster name"
  value       = yandex_airflow_cluster.airflow_cluster.name
}
