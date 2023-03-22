variable "lzy-service-image" {
  type = string
}
variable "portal_image" {
  type = string
}

resource "kubernetes_secret" "kafka_sa_key" {
  metadata {
    name = "kafka-sa-key"
  }
  data = {
    key = local.kafka-sa-key-json
  }
}

locals {
  kafka-sa-key-json = jsonencode({
    "id" : yandex_iam_service_account_key.kafka-sa-key.id
    "service_account_id" : yandex_iam_service_account_key.kafka-sa-key.service_account_id
    "created_at" : yandex_iam_service_account_key.kafka-sa-key.created_at
    "key_algorithm" : yandex_iam_service_account_key.kafka-sa-key.key_algorithm
    "public_key" : yandex_iam_service_account_key.kafka-sa-key.public_key
    "private_key" : yandex_iam_service_account_key.kafka-sa-key.private_key
  })

  lzy-service-labels = {
    app                         = "lzy-service"
    "app.kubernetes.io/name"    = "lzy-service"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "lzy-service"
  }
  lzy-service-k8s-name = "lzy-service"
  lzy-service-image    = var.lzy-service-image
}


resource "kubernetes_deployment" "lzy-service" {
  metadata {
    name   = local.lzy-service-k8s-name
    labels = local.lzy-service-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.lzy-service-labels
    }
    template {
      metadata {
        name   = local.lzy-service-k8s-name
        labels = local.lzy-service-labels
      }
      spec {
        container {
          name              = local.lzy-service-k8s-name
          image             = local.lzy-service-image
          image_pull_policy = "Always"
          port {
            container_port = local.lzy-service-port
          }
          port {
            container_port = local.lzy-service-metrics-port
          }

          env {
            name  = "LZY_SERVICE_METRICS_PORT"
            value = local.lzy-service-metrics-port
          }

          env {
            name  = "LZY_SERVICE_ADDRESS"
            value = "0.0.0.0:${local.lzy-service-port}"
          }

          env {
            name  = "LZY_SERVICE_ALLOCATOR_ADDRESS"
            value = "${kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.allocator-port}"
          }
          env {
            name  = "LZY_SERVICE_ALLOCATOR_VM_CACHE_TIMEOUT"
            value = "0s"
          }

          env {
            name  = "LZY_SERVICE_WHITEBOARD_ADDRESS"
            value = "${kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.allocator-port}"
          }

          env {
            name  = "LZY_SERVICE_GRAPH_EXECUTOR_ADDRESS"
            value = "${kubernetes_service.graph_executor_service.spec[0].cluster_ip}:${local.graph-port}"
          }

          env {
            name  = "LZY_SERVICE_CHANNEL_MANAGER_ADDRESS"
            value = "${kubernetes_service.channel_manager_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.channel-manager-port}"
          }

          env {
            name  = "LZY_SERVICE_WAIT_ALLOCATION_TIMEOUT"
            value = "10m"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_PORTAL_API_PORT"
            value = 9876
          }

          env {
            name  = "LZY_SERVICE_PORTAL_SLOTS_API_PORT"
            value = 9877
          }

          env {
            name  = "LZY_SERVICE_PORTAL_POOL_LABEL"
            value = "portals"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_POOL_ZONE"
            value = "ru-central1-a"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_DOCKER_IMAGE"
            value = var.portal_image
          }

          env {
            name  = "LZY_SERVICE_PORTAL_STDOUT_CHANNEL_NAME"
            value = "stdout"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_STDERR_CHANNEL_NAME"
            value = "stderr"
          }

          env {
            name = "LZY_SERVICE_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["lzy-service"].metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "LZY_SERVICE_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["lzy-service"].metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "LZY_SERVICE_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/lzy-service"
          }

          env {
            name  = "LZY_SERVICE_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "LZY_SERVICE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "LZY_SERVICE_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "LZY_SERVICE_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }

          env {
            name  = "LZY_SERVICE_STORAGE_ADDRESS"
            value = "${kubernetes_service.storage_service.spec[0].cluster_ip}:${local.storage-port}"
          }

          env {
            name  = "LZY_SERVICE_STORAGE_BUCKET_CREATION_TIMEOUT"
            value = "20s"
          }

          env {
            name = "LZY_SERVICE_KAFKA_ENABLED"
            value = "true"
          }

          env {
            name = "LZY_SERVICE_SCRAM_KAFKA_ENABLED"
            value = "true"
          }

          env {
            name = "LZY_SERVICE_KAFKA_BOOTSTRAP_SERVERS"
            value = "kafka.default.svc.cluster.local:9092"
          }

          env {
            name = "LZY_SERVICE_SCRAM_KAFKA_USERNAME"
            value = local.kafka_admin_username
          }

          env {
            name = "LZY_SERVICE_SCRAM_KAFKA_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.kafka_secret.metadata[0].name
                key = "password"
              }
            }
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

        dns_policy = "ClusterFirst"
        volume {
          name = "unified-agent-config"
          config_map {
            name = kubernetes_config_map.unified-agent-config["lzy-service"].metadata[0].name
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

resource "kubernetes_service" "lzy_service" {
  metadata {
    name        = "${local.lzy-service-k8s-name}-load-balancer"
    labels      = local.lzy-service-labels
    annotations = {}
  }
  spec {
    load_balancer_ip = yandex_vpc_address.workflow_public_ip.external_ipv4_address[0].address
    selector         = local.lzy-service-labels
    port {
      port        = local.lzy-service-port
      target_port = local.lzy-service-port
    }
    type = "LoadBalancer"
  }
}