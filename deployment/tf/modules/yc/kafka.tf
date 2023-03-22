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
  kafka_admin_username = "kafka_admin"
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
    name  = "auth.sasl.jaas.clientUsers[0]"
    value = local.kafka_admin_username
  }

  set {
    name  = "auth.sasl.jaas.clientPasswords[0]"
    value = random_password.kafka_password.result
  }

  set {
    name  = "zookeeper.auth.enabled"
    value = "true"
  }

  set {
    name  = "zookeeper.auth.serverUsers"
    value = "zookeeperUser"
  }

  set {
    name  = "zookeeper.auth.serverPasswords"
    value = random_password.kafka_zookeeper_password.result
  }

  set {
    name  = "zookeeper.auth.clientUser"
    value = "zookeeperUser"
  }

  set {
    name  = "zookeeper.auth.clientPassword"
    value = random_password.kafka_zookeeper_password.result
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
    value = "{User:admin,User:${local.kafka_admin_username}}"
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