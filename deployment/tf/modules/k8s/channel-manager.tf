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

resource "random_password" "channel_manager_db_passwords" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "channel_manager_db_secret" {
  metadata {
    name      = "db-secret-${local.channel-manager-k8s-name}"
    namespace = "default"
  }

  data = {
    username = local.channel-manager-k8s-name,
    password = random_password.channel_manager_db_passwords.result,
    db_host  = var.db-host
    db_port  = 6432
    db_name  = local.channel-manager-k8s-name
  }

  type = "Opaque"
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
            name  = "CHANNEL_MANAGER_METRICS_PORT"
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
                name = kubernetes_secret.channel_manager_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "CHANNEL_MANAGER_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.channel_manager_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "CHANNEL_MANAGER_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.channel_manager_db_secret.data.db_host}:${kubernetes_secret.channel_manager_db_secret.data.db_port}/${kubernetes_secret.channel_manager_db_secret.data.db_name}"
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
            value = "${kubernetes_service.lzy_service_cluster_ip.spec[0].cluster_ip}:${local.lzy-service-port}"
          }
          env {
            name = "K8S_POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name = "K8S_NAMESPACE"
            value_from {
              field_ref {
                field_path = "metadata.namespace"
              }
            }
          }
          env {
            name  = "K8S_CONTAINER_NAME"
            value = local.channel-manager-k8s-name
          }

          volume_mount {
            name       = "varloglzy"
            mount_path = "/var/log/lzy"
          }
        }
        container {
          name              = "unified-agent"
          image             = var.unified-agent-image
          image_pull_policy = "Always"
          env {
            name  = "FOLDER_ID"
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
              key  = "config"
              path = "config.yml"
            }
          }
        }
        volume {
          name = "varloglzy"
          host_path {
            path = "/var/log/lzy"
            type = "DirectoryOrCreate"
          }
        }
        node_selector = {
          type = "lzy"
        }
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 1
              pod_affinity_term {
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
}

resource "kubernetes_service" "channel_manager_service" {
  metadata {
    name        = "${local.channel-manager-k8s-name}-load-balancer"
    labels      = local.channel-manager-labels
    annotations = {
      "yandex.cloud/load-balancer-type" : "internal"
      "yandex.cloud/subnet-id" : var.custom-subnet-id
    }
  }
  spec {
    selector    = local.channel-manager-labels
    ip_families = var.channel_manager_service_ip_families
    port {
      port        = local.channel-manager-port
      target_port = local.channel-manager-port
    }
    type = "LoadBalancer"
  }
}