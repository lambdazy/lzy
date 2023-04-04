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

resource "random_password" "keystore_prefix" {
  length   = 16
  special  = false
}

resource "null_resource" "generate_keystore" {
  provisioner "local-exec" {
    command = "${path.module}/resources/generate-ssl.sh /tmp/${random_password.keystore_prefix.result}truststore.jks /tmp/${random_password.keystore_prefix.result}keystore.jks ${random_password.jks_password.result}"
  }
}

resource "local_file" "truststore" {
  filename = "truststore.jks"
  source = "/tmp/${random_password.keystore_prefix.result}truststore.jks"
  depends_on = [null_resource.generate_keystore]
}

resource "local_file" "keystore" {
  filename = "keystore.jks"
  source = "/tmp/${random_password.keystore_prefix.result}keystore.jks"
  depends_on = [null_resource.generate_keystore]
}

resource "kubernetes_secret" "kafka_jks_secret" {
  metadata {
    name = "kafka-jks-secret"
  }

  data = {
    "kafka.truststore.jks" = local_file.truststore.content_base64
    "kafka.keystore.jks" = local_file.keystore.content_base64
    "jks.password" = random_password.jks_password.result
  }

  type = "Opaque"
}

locals {
  kafka_admin_username = "admin"
  kafka_chart_config = templatefile("${path.module}/resources/kafka-config.yaml", {
    kafka_password: random_password.kafka_password.result
    subnet_id: var.subnet_id
    jks_password: random_password.jks_password.result
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