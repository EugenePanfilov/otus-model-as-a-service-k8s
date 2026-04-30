output "cluster_id" {
  value = yandex_mdb_kafka_cluster.this.id
}

output "cluster_name" {
  value = yandex_mdb_kafka_cluster.this.name
}

output "topic_names" {
  value = keys(yandex_mdb_kafka_topic.this)
}

output "usernames" {
  value = keys(yandex_mdb_kafka_user.this)
}