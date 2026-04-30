terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.199.0"
    }
  }
}

resource "yandex_kubernetes_cluster" "this" {
  name        = var.name
  description = "Managed Kubernetes cluster for fraud detection API"

  network_id = var.network_id

  service_account_id      = var.service_account_id
  node_service_account_id = var.node_service_account_id

  release_channel         = "REGULAR"
  network_policy_provider = "CALICO"

  master {
    version   = var.k8s_version
    public_ip = true

    zonal {
      zone      = var.zone
      subnet_id = var.subnet_id
    }

    security_group_ids = var.security_group_ids
  }
}

resource "yandex_kubernetes_node_group" "this" {
  name        = "${var.name}-node-group"
  description = "Worker node group for fraud detection API"

  cluster_id = yandex_kubernetes_cluster.this.id
  version    = var.k8s_version

  instance_template {
    platform_id = "standard-v3"

    resources {
      cores         = var.node_cores
      memory        = var.node_memory
      core_fraction = var.node_core_fraction
    }

    boot_disk {
      type = var.node_disk_type
      size = var.node_disk_size
    }

    network_interface {
      subnet_ids         = [var.subnet_id]
      nat                = true
      security_group_ids = var.security_group_ids
    }

    scheduling_policy {
      preemptible = var.preemptible
    }
  }

  scale_policy {
    fixed_scale {
      size = var.node_count
    }
  }

  allocation_policy {
    location {
      zone = var.zone
    }
  }

  maintenance_policy {
    auto_upgrade = true
    auto_repair  = true
  }

  depends_on = [
    yandex_kubernetes_cluster.this
  ]
}