locals {
  storage-labels = {
    app                      = "storage"
    "app.kubernetes.io/name" = "storage"
    "lzy.ai/app"             = "storage"
  }
  storage-k8s-name = "storage"
  storage-image    = var.storage-image
}

resource "kubernetes_deployment" "storage" {
  metadata {
    name   = local.storage-k8s-name
    labels = local.storage-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.storage-labels
    }
    template {
      metadata {
        name   = local.storage-k8s-name
        labels = local.storage-labels
      }
      spec {
        container {
          name              = local.storage-k8s-name
          image             = local.storage-image
          image_pull_policy = "Always"
          port {
            container_port = local.storage-port
          }

          port {
            container_port = local.storage-metrics-port
          }

          env {
            name  = "STORAGE_METRICS_PORT"
            value = local.storage-metrics-port
          }

          env {
            name  = "STORAGE_ADDRESS"
            value = "0.0.0.0:${local.storage-port}"
          }

          env {
            name = "STORAGE_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["storage"].metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "STORAGE_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["storage"].metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "STORAGE_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/storage"
          }

          env {
            name  = "STORAGE_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "STORAGE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "STORAGE_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "STORAGE_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }

          env {
            name  = "STORAGE_YC_ENABLED"
            value = "true"
          }

          env {
            name  = "STORAGE_YC_SERVICE_ACCOUNT_ID"
            value = yandex_iam_service_account.admin-sa.id
          }

          env {
            name  = "STORAGE_YC_KEY_ID"
            value = yandex_iam_service_account_key.admin-sa-key.id
          }

          env {
            name  = "STORAGE_YC_PRIVATE_KEY"
            value = yandex_iam_service_account_key.admin-sa-key.private_key
          }

          env {
            name  = "STORAGE_YC_FOLDER_ID"
            value = var.folder_id
          }

          env {
            name  = "STORAGE_S3_YC_ENABLED"
            value = "true"
          }

          env {
            name  = "STORAGE_S3_YC_ENDPOINT"
            value = "https://storage.yandexcloud.net"
          }

          env {
            name  = "STORAGE_S3_YC_ACCESS_TOKEN"
            value = yandex_iam_service_account_static_access_key.admin-sa-static-key.access_key
          }

          env {
            name  = "STORAGE_S3_YC_SECRET_TOKEN"
            value = yandex_iam_service_account_static_access_key.admin-sa-static-key.secret_key
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
            value = "storage"
          }

          volume_mount {
            name       = "varloglzy"
            mount_path = "/var/log/lzy"
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
            name = kubernetes_config_map.unified-agent-config["storage"].metadata[0].name
            items {
              key = "config"
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
      }
    }
  }
}

resource "kubernetes_service" "storage_service" {
  metadata {
    name   = "${local.storage-k8s-name}-load-balancer"
    labels = local.storage-labels
  }
  spec {
    selector = local.storage-labels
    port {
      port        = local.storage-port
      target_port = local.storage-port
    }
    type = "ClusterIP"
  }
}