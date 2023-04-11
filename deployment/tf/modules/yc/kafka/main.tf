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

    shell = {
      source = "scottwinkler/shell"
      version = "1.7.10"
    }
  }
}

resource "random_password" "kafka_password" {
  length   = 16
  special  = false
}

resource "random_password" "zookeeper_password" {
  length  = 16
  special = false
}

resource "random_password" "jks_password" {
  length   = 16
  special  = false
}

resource "shell_script" "generate_keystore" {
  lifecycle_commands {
    create = file("${path.module}/resources/generate-ssl.sh")
    delete = "echo {}"
  }

  environment = {
    JKS_PASSWORD = random_password.jks_password.result
  }

  working_directory = "${path.module}/resources"
}

locals {
  truststore_base64 = shell_script.generate_keystore.output["truststore"]
  keystore_base64 = shell_script.generate_keystore.output["keystore"]
}


resource "kubernetes_secret" "kafka_jks_secret" {
  metadata {
    name = "kafka-jks-secret"
  }

  data = {
    "jks.password" = random_password.jks_password.result
  }

  binary_data = {
    "kafka.truststore.jks" = local.truststore_base64
    "kafka.keystore.jks" = local.keystore_base64
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
    zookeeper_password: random_password.zookeeper_password.result
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