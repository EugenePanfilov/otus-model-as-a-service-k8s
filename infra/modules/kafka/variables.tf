variable "cluster_name" {
  type = string
}

variable "environment" {
  type    = string
  default = "PRODUCTION"
}

variable "network_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "zones" {
  type = list(string)
}

variable "assign_public_ip" {
  type    = bool
  default = true
}

variable "deletion_protection" {
  type    = bool
  default = false
}

variable "kafka_version" {
  type    = string
  default = "3.9"
}

variable "brokers_count" {
  type    = number
  default = 1
}

variable "resource_preset_id" {
  type    = string
  default = "s2.micro"
}

variable "disk_type_id" {
  type    = string
  default = "network-ssd"
}

variable "disk_size" {
  type    = number
  default = 20
}

variable "enable_kafka_ui" {
  type    = bool
  default = true
}

variable "enable_rest_api" {
  type    = bool
  default = false
}

variable "topics" {
  type = map(object({
    partitions         = number
    replication_factor = number
  }))
}

variable "users" {
  type = map(object({
    password = string
    permissions = list(object({
      topic_name  = string
      role        = string
      allow_hosts = optional(list(string), [])
    }))
  }))
}