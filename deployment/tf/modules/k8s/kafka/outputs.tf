output "bootstrap-servers" {
  value = "${data.kubernetes_service.kafka_bootstrap.status[0].load_balancer[0].ingress[0]["ip"]}:9092"
}

output "admin-username" {
  value = local.kafka_admin_username
}

output "admin-password" {
  value = random_password.kafka_password.result
}

output "truststore-secret-name" {
  value = "lzy-cluster-ca-cert"
}

output "truststore_certificate_pem" {
  value = data.kubernetes_secret.ca_cert.data["ca.crt"]
}

output "keystore_certificate_pem" {
  value = data.kubernetes_secret.keystore_secret.data["cluster-operator.crt"]
}

output "keystore_key_pem" {
  value = data.kubernetes_secret.keystore_secret.data["cluster-operator.key"]
}

output "keystore-secret-name" {
  value = "lzy-cluster-operator-certs"
}

output "truststore-password" {
  value = data.kubernetes_secret.ca_cert.data["ca.password"]
}

output "keystore-password" {
  value = data.kubernetes_secret.keystore_secret.data["cluster-operator.password"]
}

output "internal-bootstrap" {
  value = "lzy-kafka-brokers.default.svc:9091"
}