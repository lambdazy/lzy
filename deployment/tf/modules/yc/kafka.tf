resource "random_password" "kafka_password" {
  length   = 16
  special  = false
}

resource "random_password" "kafka_broker_password" {
  length   = 16
  special  = false
}

resource "random_password" "kafka_zookeeper_password" {
  length   = 16
  special  = false
}

locals {
  kafka_admin_username = "admin"
  kafka_chart_config = templatefile("${path.module}/configs/kafka-config.yaml", {
    kafka_password: random_password.kafka_password.result
    subnet_id: yandex_vpc_subnet.custom-subnet.id
  })
}

resource "helm_release" "lzy_kafka" {
  name       = "kafka"
  chart      = "kafka"
  repository = "https://charts.bitnami.com/bitnami"

  values = [local.kafka_chart_config]
}

data "kubernetes_service" "kafka_0_external" {
  metadata {
    name = "kafka-0-external"
    namespace = "default"
  }
}

resource "kubernetes_secret" "kafka_secret" {
  metadata {
    name      = "kafka-secret"
  }

  data = {
    username = local.kafka_admin_username,
    password = random_password.kafka_password.result
  }

  type = "Opaque"
}