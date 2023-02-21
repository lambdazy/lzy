output "whiteboard-address" {
  value = "${yandex_vpc_address.whiteboard_public_ip.external_ipv4_address[0].address}:${local.whiteboard-port}"
}

output "lzy-service-address" {
  value = "${yandex_vpc_address.workflow_public_ip.external_ipv4_address[0].address}:${local.lzy-service-port}"
}