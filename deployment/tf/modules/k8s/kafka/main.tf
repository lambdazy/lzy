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

    kubectl = {
      source = "gavinbunney/kubectl"
      version = ">= 1.7.0"
    }
  }
}

resource "random_password" "kafka_password" {
  length   = 16
  special  = false
}

locals {
  kafka_admin_username = "admin"
}

resource "helm_release" "strimzi_operator" {
  name  = "strimzi"
  chart = "strimzi-kafka-operator"
  repository = "https://strimzi.io/charts/"
  reuse_values = true
  namespace = "default"
  cleanup_on_fail = true
  force_update = true

  values = [file("${path.module}/resources/strimzi-config.yaml")]
}

resource "kubectl_manifest" "kafka" {


  yaml_body = templatefile("${path.module}/resources/kafka-service.yaml", {
    subnet_id: var.subnet_id
    username: local.kafka_admin_username
  })

  depends_on = [helm_release.strimzi_operator, kubernetes_config_map.kafka_config_map]
}

resource "kubernetes_secret" "admin_password" {
  metadata {
    name = "admin-password"
  }

  data = {
    password: random_password.kafka_password.result
  }
}


data "kubernetes_service" "kafka_bootstrap" {
  metadata {
    name = "lzy-kafka-scram-bootstrap"
  }

  depends_on = [kubectl_manifest.kafka]
}


data "kubernetes_service" "kafka_internal_bootstrap" {
  metadata {
    name = "lzy-kafka-internal-bootstrap"
  }

  depends_on = [kubectl_manifest.kafka]
}


data "kubernetes_secret" "ca_cert" {
  metadata {
    name = "lzy-cluster-ca-cert"
    namespace = "default"
  }

  depends_on = [kubectl_manifest.kafka]
}

data "kubernetes_secret" "keystore_secret" {
  metadata {
    name = "lzy-cluster-operator-certs"
    namespace = "default"
  }

  depends_on = [kubectl_manifest.kafka]
}