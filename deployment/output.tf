output "kube_config" {
  value = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive = true
}

output "host" {
  value = azurerm_kubernetes_cluster.main.kube_config[0].host
  sensitive = true
}

output "lzy_server_host" {
  value = kubernetes_pod.lzy-server.spec[0].host_ipc
}

output "lzy_kharon_domain_name_label" {
  value = azurerm_public_ip.test.domain_name_label
}

output "lzy_kharon_public_ip_address" {
  value = azurerm_public_ip.test.ip_address
}
