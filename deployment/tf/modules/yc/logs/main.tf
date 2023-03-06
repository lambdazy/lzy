terraform {
  required_providers {
    kubernetes = {
      version = ">=2.11.0"
    }
  }
}

variable "node-sa-key" {
  type = object({
    id : string
    service_account_id : string
    created_at : string
    key_algorithm : string
    public_key : string
    private_key : string
  })
}

variable "folder_id" {
  type = string
}

locals {
  fluentd-sa-key-json = jsonencode(var.node-sa-key)
}

resource "kubernetes_namespace" "logging" {
  metadata {
    name = "logging"
    labels = {
      k8s-app: "fluent-bit"
    }
  }
}

resource "kubernetes_secret" "fluentd_sa_key" {
  metadata {
    name = "fluentd-sa-key"
    namespace = kubernetes_namespace.logging.metadata[0].name
  }
  data = {
    key = local.fluentd-sa-key-json
  }
}

resource "kubernetes_service_account" "fluentd_sa" {

  metadata {
    name = "fluent-bit"
    namespace = kubernetes_namespace.logging.metadata[0].name
    labels = {
      k8s-app: "fluent-bit"
    }
  }
}

resource "kubernetes_cluster_role" "fluentd_cluster_role" {

  metadata {
    name = "fluent-bit-read"
    labels = {
      k8s-app: "fluent-bit"
    }
  }
  rule {
    api_groups = [""]
    verbs = ["get", "list", "watch"]
    resources = ["namespaces", "pods"]
  }
}

resource "kubernetes_cluster_role_binding" "fluentd_cluster_role_binding" {

  metadata {
    name = "fluent-bit-read"
    labels = {
      k8s-app: "fluent-bit"
    }
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.fluentd_cluster_role.metadata[0].name
  }
  subject {
    kind = "ServiceAccount"
    name = kubernetes_service_account.fluentd_sa.metadata[0].name
    namespace = kubernetes_namespace.logging.metadata[0].name
  }
}

resource "kubernetes_config_map" "fluent_bit_config_map" {

  metadata {
    name = "fluent-bit-config"
    namespace = kubernetes_namespace.logging.metadata[0].name
    labels = {
      k8s-app: "fluent-bit"
    }
  }

  data = {
    "fluent-bit.conf": <<-EOT
      [SERVICE]
          Flush         1
          Log_Level     info
          Daemon        off
          Parsers_File  parsers.conf
          HTTP_Server   On
          HTTP_Listen   0.0.0.0
          HTTP_Port     2020

      @INCLUDE input-kubernetes.conf
      @INCLUDE filter-kubernetes.conf
      @INCLUDE output-elasticsearch.conf
      EOT

    "input-kubernetes.conf": <<-EOT
      [INPUT]
          Name              tail
          Tag               kube.*
          Path              /var/log/containers/*default*.log
          Parser            docker
          DB                /var/log/flb_kube.db
          Mem_Buf_Limit     5MB
          Refresh_Interval  10
      EOT

    "parsers.conf": <<-EOT
      [PARSER]
          NAME lzy-log
          Format regex
          Regex ^[^ ]*\s*[^ ]*\s*\w*\s*(?<time>[^ ]*\s*[^ ]*)\s*\[(?<thread>[^\]]+)\]\s*(?<level>[^ ]+)\s*(?<logger>[^ ]+)[^-]*-\s*(?<msg>.*)

      [PARSER]
          Name        docker
          Format      json
          Time_Key    time
          Time_Format %Y-%m-%dT%H:%M:%S.%L
          Time_Keep   On
      EOT

    "filter-kubernetes.conf": <<-EOT

      [FILTER]
          Name                parser
          Match               kube*
          Parser              lzy-log
          Key_Name            log
          Reserve_Data        On

      [FILTER]
          Name                kubernetes
          Match               kube.*
          Merge_Log           On
          Merge_Log_Key       log_processed
          K8S-Logging.Parser  On
          K8S-Logging.Exclude Off
      EOT

    "output-elasticsearch.conf": <<-EOT

      [OUTPUT]
        Name            yc-logging
        Match           *
        resource_id     {kubernetes/container_name}
        message_key     msg
        level_key       level
        folder_id       ${var.folder_id}
        authorization   iam-key-file:/etc/secret/sa-key.json

      [OUTPUT]
        Name            stdout
        Match           *
      EOT
  }
}


resource "kubernetes_daemonset" "fluent_bit" {

  metadata {
    name = "fluent-bit"
    namespace = kubernetes_namespace.logging.metadata[0].name
    labels = {
      "k8s-app": "fluent-bit-logging"
      "kubernetes.io/cluster-service": "true"
    }

  }
  spec {
    selector {
      match_labels = {
        "k8s-app": "fluent-bit-logging"
      }
    }
    template {
      metadata {
        labels = {
          "k8s-app": "fluent-bit-logging"
          "kubernetes.io/cluster-service": "true"
        }
      }

      spec {

        node_selector = {
          "lzy.ai/logging_allowed" = "true"
        }

        container {
          name = "fluent-bit"
          image = "cr.yandex/yc/fluent-bit-plugin-yandex:v2.0.3-fluent-bit-1.9.3"
          image_pull_policy = "Always"

          volume_mount {
            mount_path = "/var/log"
            name       = "varlog"
          }

          volume_mount {
            mount_path = "/var/lib/docker/containers"
            name       = "varlibdockercontainers"
            read_only = true
          }

          volume_mount {
            mount_path = "/fluent-bit/etc/"
            name       = "fluent-bit-config"
          }

          volume_mount {
            mount_path = "/etc/secret"
            name       = "secret-key-json-volume"
          }
        }

        termination_grace_period_seconds = 10

        volume {
          name = "varlog"
          host_path {
            path = "/var/log"
          }
        }

        volume {
          name = "varlibdockercontainers"
          host_path {
            path = "/var/lib/docker/containers"
          }
        }

        volume {
          name = "fluent-bit-config"
          config_map {
            name = kubernetes_config_map.fluent_bit_config_map.metadata[0].name
          }
        }

        volume {
          name = "secret-key-json-volume"
          secret {
            secret_name = kubernetes_secret.fluentd_sa_key.metadata[0].name
            items {
              key  = "key"
              path = "sa-key.json"
            }
          }
        }

        service_account_name = kubernetes_service_account.fluentd_sa.metadata[0].name

        toleration {
          key = "node-role.kubernetes.io/master"
          operator = "Exists"
          effect = "NoSchedule"
        }

        toleration {
          operator = "Exists"
          effect = "NoExecute"
        }

        toleration {
          operator = "Exists"
          effect = "NoSchedule"
        }

      }
    }
  }
}