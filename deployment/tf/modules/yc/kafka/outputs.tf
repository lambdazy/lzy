output "bootstrap-servers" {
  value = "${data.kubernetes_service.kafka_0_external.status[0].load_balancer[0].ingress[0]["ip"]}:9094"
}

output "admin-username" {
  value = local.kafka_admin_username
}

output "admin-password" {
  value = random_password.kafka_password.result
}

output "jks-secret-name" {
  value = "kafka-jks-secret"
}