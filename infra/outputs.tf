/*
output "kafka_cluster_id" {
  value = module.kafka.cluster_id
}

output "kafka_cluster_name" {
  value = module.kafka.cluster_name
}

output "kafka_topic_names" {
  value = module.kafka.topic_names
}

output "kafka_usernames" {
  value = module.kafka.usernames
}
*/

# output "external_ip_address_vm_1" {
#   value = module.compute.external_ip_address
# }

output "k8s_cluster_id" {
  description = "Managed Kubernetes cluster ID"
  value       = module.k8s.cluster_id
}

output "k8s_cluster_name" {
  description = "Managed Kubernetes cluster name"
  value       = module.k8s.cluster_name
}

output "k8s_node_group_id" {
  description = "Managed Kubernetes node group ID"
  value       = module.k8s.node_group_id
}

output "airflow_cluster_id" {
  description = "Managed Airflow cluster ID"
  value       = module.airflow-cluster.cluster_id
}

output "airflow_cluster_name" {
  description = "Managed Airflow cluster name"
  value       = module.airflow-cluster.cluster_name
}
/*
output "kafka_cluster_id" {
  description = "Managed Kafka cluster ID"
  value       = module.kafka.cluster_id
}

output "kafka_cluster_name" {
  description = "Managed Kafka cluster name"
  value       = module.kafka.cluster_name
}

output "kafka_topic_names" {
  description = "Kafka topic names"
  value       = module.kafka.topic_names
}

output "kafka_usernames" {
  description = "Kafka usernames"
  value       = module.kafka.usernames
}
*/