variable "name" {
  description = "Kubernetes cluster name"
  type        = string
}

variable "network_id" {
  description = "Yandex VPC network ID"
  type        = string
}

variable "subnet_id" {
  description = "Yandex VPC subnet ID"
  type        = string
}

variable "zone" {
  description = "Yandex Cloud availability zone"
  type        = string
}

variable "service_account_id" {
  description = "Service account ID for Kubernetes control plane"
  type        = string
}

variable "node_service_account_id" {
  description = "Service account ID for Kubernetes nodes"
  type        = string
}

variable "security_group_ids" {
  description = "Security group IDs for Kubernetes cluster and nodes"
  type        = list(string)
  default     = []
}

variable "k8s_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.30"
}

variable "node_count" {
  description = "Number of Kubernetes worker nodes"
  type        = number
  default     = 3
}

variable "node_cores" {
  description = "CPU cores per node"
  type        = number
  default     = 2
}

variable "node_memory" {
  description = "RAM per node in GB"
  type        = number
  default     = 4
}

variable "node_core_fraction" {
  description = "Core fraction for nodes"
  type        = number
  default     = 20
}

variable "node_disk_size" {
  description = "Boot disk size per node in GB"
  type        = number
  default     = 64
}

variable "node_disk_type" {
  description = "Boot disk type"
  type        = string
  default     = "network-hdd"
}

variable "preemptible" {
  description = "Use preemptible nodes"
  type        = bool
  default     = true
}