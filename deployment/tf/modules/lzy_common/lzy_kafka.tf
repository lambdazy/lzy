resource "random_password" "lzy_kafka_password" {
  count   = 2
  length  = 16
  special = false
}

resource "kubernetes_secret" "lzy_kafka_servant" {
  metadata {
    name = "kafka-servant"
  }

  data = {
    username = "servant"
    password = random_password.lzy_kafka_password[0].result
  }

  type = "Opaque"
}

resource "kubernetes_secret" "lzy_kafka_clickhouse" {
  metadata {
    name = "kafka-clickhouse"
  }

  data = {
    username = "clickhouse"
    password = random_password.lzy_kafka_password[1].result
  }

  type = "Opaque"
}

resource "helm_release" "lzy_kafka" {
  name       = "kafka"
  chart      = "kafka"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name  = "image.debug"
    value = "true"
  }

  set {
    name  = "metadata.labels.app"
    value = "kafka"
  }

  values = [
    file("lzy_node_selector_and_pod_anti_affinity.yaml")
  ]

  depends_on = [kubernetes_secret.lzy_kafka_clickhouse]
}