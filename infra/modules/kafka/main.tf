resource "yandex_mdb_kafka_cluster" "this" {
  name                = var.cluster_name
  environment         = var.environment
  network_id          = var.network_id
  subnet_ids          = var.subnet_ids
  security_group_ids  = var.security_group_ids
  deletion_protection = var.deletion_protection

  config {
    version          = var.kafka_version
    brokers_count    = var.brokers_count
    zones            = var.zones
    assign_public_ip = var.assign_public_ip
    schema_registry  = false

    rest_api {
      enabled = var.enable_rest_api
    }

    kafka_ui {
      enabled = var.enable_kafka_ui
    }

    kafka {
      resources {
        resource_preset_id = var.resource_preset_id
        disk_type_id       = var.disk_type_id
        disk_size          = var.disk_size
      }

      kafka_config {
        auto_create_topics_enable  = false
        default_replication_factor = 1
        num_partitions             = 3
        compression_type           = "COMPRESSION_TYPE_ZSTD"
        sasl_enabled_mechanisms = [
          "SASL_MECHANISM_SCRAM_SHA_256",
          "SASL_MECHANISM_SCRAM_SHA_512"
        ]
      }
    }
  }

  timeouts {
    create = "1h30m"
    update = "2h"
    delete = "30m"
  }
}

resource "yandex_mdb_kafka_topic" "this" {
  for_each = var.topics

  cluster_id         = yandex_mdb_kafka_cluster.this.id
  name               = each.key
  partitions         = each.value.partitions
  replication_factor = each.value.replication_factor

  topic_config {
    cleanup_policy   = "CLEANUP_POLICY_DELETE"
    compression_type = "COMPRESSION_TYPE_ZSTD"
  }
}

resource "yandex_mdb_kafka_user" "this" {
  for_each = var.users

  cluster_id = yandex_mdb_kafka_cluster.this.id
  name       = each.key
  password   = each.value.password

  dynamic "permission" {
    for_each = each.value.permissions
    content {
      topic_name  = permission.value.topic_name
      role        = permission.value.role
      allow_hosts = try(permission.value.allow_hosts, [])
    }
  }
}