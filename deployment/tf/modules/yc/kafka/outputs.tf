output "bootstrap-servers" {
  value = "${data.kubernetes_service.kafka_bootstrap.status[0].load_balancer[0].ingress[0]["ip"]}:9092"
}

output "admin-username" {
  value = local.kafka_admin_username
}

output "admin-password" {
  value = random_password.kafka_password.result
}

output "jks-secret-name" {
  value = "lzy-cluster-ca-cert"
}

output "jks-password" {
  value = data.kubernetes_secret.ca_cert.data["ca.password"]
}