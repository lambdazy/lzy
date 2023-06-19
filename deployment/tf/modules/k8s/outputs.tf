output "db_data" {
  value = map(
    kubernetes_secret.allocator_db_secret.data.db_name, kubernetes_secret.allocator_db_secret.data.password,
    kubernetes_secret.channel_manager_db_secret.data.db_name, kubernetes_secret.channel_manager_db_secret.data.password,
    kubernetes_secret.graph_executor_db_secret.data.db_name, kubernetes_secret.graph_executor_db_secret.data.password,
    kubernetes_secret.iam_db_secret.data.db_name, kubernetes_secret.iam_db_secret.data.password,
    kubernetes_secret.lzy_service_db_secret.data.db_name, kubernetes_secret.lzy_service_db_secret.data.password,
    kubernetes_secret.scheduler_db_secret.data.db_name, kubernetes_secret.scheduler_db_secret.data.password,
    kubernetes_secret.storage_db_secret.data.db_name, kubernetes_secret.storage_db_secret.data.password,
    kubernetes_secret.whiteboard_db_secret.data.db_name, kubernetes_secret.whiteboard_db_secret.data.password
  )
}

output "allocator_address" {
  value = kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]
}