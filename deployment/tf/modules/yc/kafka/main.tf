terraform {
  required_providers {
    kubernetes = {
      version = ">=2.11.0"
    }
    helm = {
      version = ">=2.5.1"
    }
    random = {
      version = ">=3.0.1"
    }
    tls = {
      version = ">=3.4.0"
    }
  }
}

resource "random_password" "kafka_password" {
  length   = 16
  special  = false
}

resource "random_password" "jks_password" {
  length   = 16
  special  = false
}

resource "random_password" "jks_prefix" {
  length   = 16
  special  = false
}

resource "null_resource" "generate_jks" {
  provisioner "local-exec" {
    command = "${path.module}/resources/generate-ssl.sh /tmp/${random_password.jks_prefix}truststore.jks /tmp/${random_password.jks_prefix}keystore.jks ${random_password.jks_password}"
  }
}

resource "kubernetes_secret" "kafka_jks_secret" {
  metadata {
    name = "kafka-jks-secret"
  }

  data = {
    kafka.truststore.jks = filebase64("/tmp/${random_password.jks_prefix}truststore.jks")
    kafka.keystore.jks = filebase64("/tmp/${random_password.jks_prefix}keystore.jks")
    jks.password = random_password.jks_password.result
  }
}

resource "tls_private_key" "kafka_private_key" {
  algorithm = "ECDSA"
}

resource "tls_self_signed_cert" "kafka_tls_cert" {

  allowed_uses          = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]

  key_algorithm         = tls_private_key.kafka_private_key.algorithm
  private_key_pem       = tls_private_key.kafka_private_key.private_key_pem
  validity_period_hours = 2400
  early_renewal_hours = 1200

  subject {
    common_name = "kafka.lzy.ai"
    organization = "Lzy"
  }
}

locals {
  kafka_admin_username = "admin"
  kafka_chart_config = templatefile("${path.module}/resources/kafka-config.yaml", {
    kafka_password: random_password.kafka_password.result
    subnet_id: var.subnet_id
    jks_password: random_password.jks_password
    jks_secret_name: kubernetes_secret.kafka_jks_secret.metadata[0].name
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

  depends_on = [helm_release.lzy_kafka]
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