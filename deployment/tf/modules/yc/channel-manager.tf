locals {
  channel-manager-labels = {
    app                         = "channel-manager"
    "app.kubernetes.io/name"    = "channel-manager"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "channel-manager"
  }
  channel-manager-k8s-name = "channel-manager"
  channel-manager-image    = var.channel-manager-image
}

resource "kubernetes_deployment" "channel-manager" {
  metadata {
    name   = local.channel-manager-k8s-name
    labels = local.channel-manager-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.channel-manager-labels
    }
    template {
      metadata {
        name   = local.channel-manager-k8s-name
        labels = local.channel-manager-labels
      }
      spec {
        container {
          name              = local.channel-manager-k8s-name
          image             = local.channel-manager-image
          image_pull_policy = "Always"
          port {
            container_port = local.channel-manager-port
          }
          port {
            container_port = local.channel-manager-metrics-port
          }

          env {
            name = "CHANNEL_MANAGER_METRICS_PORT"
            value = local.channel-manager-metrics-port
          }
          env {
            name  = "CHANNEL_MANAGER_ADDRESS"
            value = "0.0.0.0:${local.channel-manager-port}"
          }

          env {
            name = "CHANNEL_MANAGER_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["channel-manager"].metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "CHANNEL_MANAGER_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["channel-manager"].metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "CHANNEL_MANAGER_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/channel-manager"
          }

          env {
            name  = "CHANNEL_MANAGER_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "CHANNEL_MANAGER_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "CHANNEL_MANAGER_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "CHANNEL_MANAGER_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }
          env {
            name  = "CHANNEL_MANAGER_LZY_SERVICE_ADDRESS"
            value = "${kubernetes_service.lzy_service.spec[0].cluster_ip}:${local.lzy-service-port}"
          }
          env {
            name  = "CHANNEL_MANAGER_WHITEBOARD_ADDRESS"
            value = "http://${kubernetes_service.whiteboard_service.spec[0].cluster_ip}:${local.whiteboard-port}"
          }
        }
        container {
          name = "unified-agent"
          image = var.unified-agent-image
          image_pull_policy = "Always"
          env {
            name = "FOLDER_ID"
            value = var.folder_id
          }
          volume_mount {
            name       = "unified-agent-config"
            mount_path = "/etc/yandex/unified_agent/conf.d/"
          }
        }
        volume {
          name = "unified-agent-config"
          config_map {
            name = kubernetes_config_map.unified-agent-config["channel-manager"].metadata[0].name
            items {
              key = "config"
              path = "config.yml"
            }
          }
        }
        node_selector = {
          type = "lzy"
        }
        affinity {
          pod_anti_affinity {
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_expressions {
                  key      = "app.kubernetes.io/part-of"
                  operator = "In"
                  values   = ["lzy"]
                }
              }
              topology_key = "kubernetes.io/hostname"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "channel_manager_service" {
  metadata {
    name   = "${local.channel-manager-k8s-name}-load-balancer"
    labels = local.channel-manager-labels
    annotations = {
      "yandex.cloud/load-balancer-type" : "internal"
      "yandex.cloud/subnet-id" : yandex_vpc_subnet.custom-subnet.id
    }
  }
  spec {
    selector = local.channel-manager-labels
    port {
      port        = local.channel-manager-port
      target_port = local.channel-manager-port
    }
    type = "LoadBalancer"
  }
}