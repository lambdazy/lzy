output "whiteboard-address" {
  value = "${yandex_vpc_address.whiteboard_public_ip.external_ipv4_address[0].address}:${module.k8s_deployment.whiteboard_port}"
}

output "lzy-service-address" {
  value = "${yandex_vpc_address.workflow_public_ip.external_ipv4_address[0].address}:${module.k8s_deployment.lzy_service_port}"
}