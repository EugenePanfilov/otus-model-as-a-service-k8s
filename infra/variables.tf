variable "yc_instance_user" {
  type = string
}

variable "yc_instance_name" {
  type = string
}

variable "yc_network_name" {
  type = string
}

variable "yc_subnet_name" {
  type = string
}

variable "yc_service_account_name" {
  type = string
}

variable "yc_bucket_name" {
  type = string
}

variable "yc_storage_endpoint_url" {
  type    = string
  default = "https://storage.yandexcloud.net"
}

variable "ubuntu_image_id" {
  type = string
}

variable "public_key_path" {
  type = string
}

variable "private_key_path" {
  type = string
}

variable "admin_password" {
  type        = string
  description = "Admin password for the Airflow web interface"
}

variable "yc_config" {
  type = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
  description = "Yandex Cloud configuration"
}

# MLflow variables
variable "yc_mlflow_instance_name" {
  type        = string
  description = "Name of the MLflow server instance"
}

# PostgreSQL variables
variable "yc_postgres_cluster_name" {
  type        = string
  description = "Name of the PostgreSQL cluster"
}

variable "postgres_password" {
  type        = string
  description = "Password for PostgreSQL database used by MLflow"
  sensitive   = true
}

variable "kafka_cluster_name" {
  type    = string
  default = "fraud-kafka-cluster"
}

variable "kafka_version" {
  type    = string
  default = "3.9"
}

variable "kafka_brokers_count" {
  type    = number
  default = 1
}

variable "kafka_resource_preset_id" {
  type    = string
  default = "s2.micro"
}

variable "kafka_disk_type_id" {
  type    = string
  default = "network-ssd"
}

variable "kafka_disk_size" {
  type    = number
  default = 20
}

variable "kafka_assign_public_ip" {
  type    = bool
  default = true
}

variable "kafka_enable_kafka_ui" {
  type    = bool
  default = true
}

variable "kafka_enable_rest_api" {
  type    = bool
  default = false
}

variable "kafka_topics" {
  type = map(object({
    partitions         = number
    replication_factor = number
  }))
}

variable "kafka_users" {
  type = map(object({
    password = string
    permissions = list(object({
      topic_name  = string
      role        = string
      allow_hosts = optional(list(string), [])
    }))
  }))
}

variable "k8s_cluster_name" {
  description = "Managed Kubernetes cluster name"
  type        = string
  default     = "fraud-detection-k8s"
}

variable "k8s_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.31"
}

variable "k8s_node_count" {
  description = "Number of worker nodes"
  type        = number
  default     = 1
}

variable "k8s_node_cores" {
  description = "CPU cores per Kubernetes node"
  type        = number
  default     = 2
}

variable "k8s_node_memory" {
  description = "RAM per Kubernetes node in GB"
  type        = number
  default     = 4
}

variable "k8s_node_core_fraction" {
  description = "Core fraction per Kubernetes node"
  type        = number
  default     = 20
}

variable "k8s_node_disk_size" {
  description = "Boot disk size per Kubernetes node in GB"
  type        = number
  default     = 64
}

variable "k8s_preemptible_nodes" {
  description = "Use preemptible Kubernetes nodes"
  type        = bool
  default     = true
}