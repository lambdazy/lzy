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
}

resource "helm_release" "lzy_kafka" {
  name       = "kafka"
  chart      = "kafka"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name = "replicaCount"
    value = "1"
  }

  set {
    name  = "auth.clientProtocol"
    value = "sasl"
  }

  set {
    name = "auth.interBrokerProtocol"
    value = "sasl"
  }

  set {
    name  = "auth.sasl.jaas.interBrokerPassword"
    value = random_password.kafka_password.result
  }

  set {
    name  = "zookeeper.auth.client.enabled"
    value = "true"
  }

  set {
    name  = "zookeeper.auth.client.serverUsers"
    value = "admin"
  }

  set {
    name  = "zookeeper.auth.client.serverPasswords"
    value = random_password.kafka_password.result
  }

  set {
    name  = "auth.sasl.jaas.zookeeperUser"
    value = "admin"
  }

  set {
    name  = "auth.sasl.jaas.zookeeperPassword"
    value = random_password.kafka_password.result
  }

  set {
    name  = "zookeeper.auth.client.clientUser"
    value = "admin"
  }

  set {
    name  = "zookeeper.auth.client.clientPassword"
    value = random_password.kafka_password.result
  }

  set {
    name  = "authorizerClassName"
    value = "kafka.security.authorizer.AclAuthorizer"
  }

  set {
    name  = "allowEveryoneIfNoAclFound"
    value = "false"
  }

  set {
    name  = "superUsers"
    value = "User:${local.kafka_admin_username}"
  }

  set {
    name  = "deleteTopicEnable"
    value = "true"
  }

  set {
    name  = "autoCreateTopicsEnable"
    value = "false"
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