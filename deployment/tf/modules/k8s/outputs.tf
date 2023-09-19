output "db_data" {
  value = tomap({
    "${local.allocator-k8s-name}" = kubernetes_secret.allocator_db_secret.data.password,
    "${local.channel-manager-k8s-name}" = kubernetes_secret.channel_manager_db_secret.data.password,
    "${local.graph-k8s-name}" = kubernetes_secret.graph_executor_db_secret.data.password,
    "${local.iam-k8s-name}" = kubernetes_secret.iam_db_secret.data.password,
    "${local.lzy-service-k8s-name}" = kubernetes_secret.lzy_service_db_secret.data.password,
    "${local.scheduler-k8s-name}" = kubernetes_secret.scheduler_db_secret.data.password,
    "${local.whiteboard-k8s-name}" = kubernetes_secret.whiteboard_db_secret.data.password
  })
}

output "allocator_address" {
  value = kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]
}

output "whiteboard_port" {
  value = local.whiteboard-port
}

output "lzy_service_port" {
  value = local.lzy-service-port
}

output "allocator_cluster_ip" {
  value = kubernetes_service.allocator_service_cluster_ip.spec[0].cluster_ip
}