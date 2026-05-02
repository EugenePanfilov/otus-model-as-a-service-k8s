output "network_id" {
  value = yandex_vpc_network.network.id
}

output "subnet_id" {
  value = yandex_vpc_subnet.subnet.id
}

output "security_group_id" {
  value = yandex_vpc_security_group.security_group.id
}
/*
output "kafka_security_group_id" {
  value = yandex_vpc_security_group.kafka_security_group.id
}
*/
output "route_table_id" {
  value = yandex_vpc_route_table.route_table.id
}

output "nat_gateway_id" {
  value = yandex_vpc_gateway.nat_gateway.id
}
