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

provider "kubectl" {
  host = var.kuber_host
  cluster_ca_certificate = var.cluster_ca_certificate
  token = var.cluster_token
  load_config_file = false
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

  values = [file("${path.module}/resources/strimzi-config.yaml")]
}

resource "kubectl_manifest" "kafka" {


  yaml_body = templatefile("${path.module}/resources/kafka-service.yaml", {
    subnet_id: var.subnet_id
  })

  depends_on = [helm_release.strimzi_operator]
}

resource "kubernetes_secret" "admin_password" {
  metadata {
    name = "admin-password"
  }

  data = {
    password: random_password.kafka_password.result
  }
}

resource "kubectl_manifest" "admin_user" {
  yaml_body = templatefile("${path.module}/resources/kafka-admin-user.yaml", {
    username: local.kafka_admin_username
    password_secret: "admin-password"
    password_secret_key: "password"
  })

  depends_on = [kubectl_manifest.kafka]
}


data "kubernetes_service" "kafka_bootstrap" {
  metadata {
    name = "lzy-kafka-scram-bootstrap"
  }

  depends_on = [kubectl_manifest.kafka]
}

data "kubernetes_secret" "ca_cert" {
  metadata {
    name = "lzy-cluster-ca-cert"
    namespace = "default"
  }
}