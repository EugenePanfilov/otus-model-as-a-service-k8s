output "cluster_id" {
  description = "Kubernetes cluster ID"
  value       = yandex_kubernetes_cluster.this.id
}

output "cluster_name" {
  description = "Kubernetes cluster name"
  value       = yandex_kubernetes_cluster.this.name
}

output "node_group_id" {
  description = "Kubernetes node group ID"
  value       = yandex_kubernetes_node_group.this.id
}