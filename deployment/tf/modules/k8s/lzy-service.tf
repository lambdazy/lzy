locals {
  lzy-service-labels = {
    app                         = "lzy-service"
    "app.kubernetes.io/name"    = "lzy-service"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "lzy-service"
  }
  lzy-service-k8s-name = "lzy-service"
  lzy-service-image    = var.lzy-service-image
}

resource "random_password" "lzy_service_db_passwords" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "lzy_service_db_secret" {
  metadata {
    name      = "db-secret-${local.lzy-service-k8s-name}"
    namespace = "default"
  }

  data = {
    username = local.lzy-service-k8s-name,
    password = random_password.lzy_service_db_passwords.result,
    db_host  = var.db-host
    db_port  = 6432
    db_name  = local.lzy-service-k8s-name
  }

  type = "Opaque"
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
            value = "${kubernetes_service.allocator_service_cluster_ip.spec[0].cluster_ip}:${local.allocator-port}"
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
            name = "LZY_SERVICE_GC_ENABLED"
            value = "true"
          }

          env {
            name = "LZY_SERVICE_GC_PERIOD"
            value = "30s"
          }

          env {
            name = "LZY_SERVICE_GC_LEADER_PERIOD"
            value = "1m"
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
            name  = "LZY_SERVICE_PORTAL_WORKERS_POOL_SIZE"
            value = "10"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_DOWNLOADS_POOL_SIZE"
            value = "5"
          }

          env {
            name  = "LZY_SERVICE_PORTAL_CHUNKS_POOL_SIZE"
            value = "5"
          }

          env {
            name = "LZY_SERVICE_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.lzy_service_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "LZY_SERVICE_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.lzy_service_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "LZY_SERVICE_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.lzy_service_db_secret.data.db_host}:${kubernetes_secret.lzy_service_db_secret.data.db_port}/${kubernetes_secret.lzy_service_db_secret.data.db_name}"
          }

          env {
            name  = "LZY_SERVICE_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "LZY_SERVICE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.status[0].load_balancer[0].ingress[0]["ip"]}:${local.iam-port}"
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
            name  = "LZY_SERVICE_BUCKET_CREATION_TIMEOUT"
            value = "20s"
          }

          env {
            name  = "LZY_SERVICE_STORAGE_YC_ENABLED"
            value = "true"
          }

          env {
            name  = "LZY_SERVICE_STORAGE_YC_FOLDER_ID"
            value = var.folder_id
          }

          env {
            name  = "LZY_SERVICE_STORAGE_YC_ENDPOINT"
            value = var.yc-endpoint
          }

          env {
            name  = "LZY_SERVICE_STORAGE_S3_YC_ENABLED"
            value = "true"
          }

          env {
            name  = "LZY_SERVICE_STORAGE_S3_YC_ENDPOINT"
            value = "https://storage.yandexcloud.net"
          }

          env {
            name  = "LZY_SERVICE_STORAGE_S3_YC_ACCESS_TOKEN"
            value = var.node-sa-static-key-access-key
          }

          env {
            name  = "LZY_SERVICE_STORAGE_S3_YC_SECRET_TOKEN"
            value = var.node-sa-static-key-secret-key
          }

          dynamic "env" {
            for_each = local.kafka_env_map

            content {
              name  = "LZY_SERVICE_${env.key}"
              value = env.value
            }
          }

          dynamic "env" {
            for_each = var.enable_kafka ? [1] : []
            content {
              name  = "LZY_SERVICE_KAFKA_BOOTSTRAP_SERVERS"
              value = module.kafka[0].internal-bootstrap
            }
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
            value = local.lzy-service-k8s-name
          }

          dynamic "env" {
            for_each = var.s3_sink_enabled ? [1] : []

            content {
              name  = "LZY_SERVICE_S3_SINK_ADDRESS"
              value = "${kubernetes_service.s3_sink_service[0].spec[0].cluster_ip}:${local.s3-sink-port}"
            }
          }

          env {
            name = "LZY_SERVICE_OPERATIONS_START_WORKFLOW_TIMEOUT"
            value = "10s"
          }
          env {
            name = "LZY_SERVICE_OPERATIONS_FINISH_WORKFLOW_TIMEOUT"
            value = "10m"
          }
          env {
            name = "LZY_SERVICE_OPERATIONS_ABORT_WORKFLOW_TIMEOUT"
            value = "10m"
          }
          env {
            name = "LZY_SERVICE_OPERATIONS_EXECUTE_GRAPH_TIMEOUT"
            value = "10s"
          }

          volume_mount {
            name       = "varloglzy"
            mount_path = "/var/log/lzy"
          }

          dynamic "volume_mount" {
            for_each = var.enable_kafka ? [1] : []

            content {
              mount_path = "/truststore"
              name       = "truststore"
            }
          }
          dynamic "volume_mount" {
            for_each = var.enable_kafka ? [1] : []

            content {
              mount_path = "/keystore"
              name       = "keystore"
            }
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

        dns_policy = "ClusterFirst"
        volume {
          name = "unified-agent-config"
          config_map {
            name = kubernetes_config_map.unified-agent-config["lzy-service"].metadata[0].name
            items {
              key  = "config"
              path = "config.yml"
            }
          }
        }

        dynamic "volume" {
          for_each = var.enable_kafka ? [1] : []

          content {
            name = "truststore"
            secret {
              secret_name = module.kafka[0].truststore-secret-name
              items {
                key  = "ca.p12"
                path = "truststore.p12"
              }
            }
          }
        }

        dynamic "volume" {
          for_each = var.enable_kafka ? [1] : []

          content {
            name = "keystore"
            secret {
              secret_name = module.kafka[0].keystore-secret-name
              items {
                key  = "cluster-operator.p12"
                path = "keystore.p12"
              }
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

resource "kubernetes_service" "lzy_service" {
  metadata {
    name        = "${local.lzy-service-k8s-name}-load-balancer"
    labels      = local.lzy-service-labels
    annotations = {}
  }
  spec {
    load_balancer_ip = var.workflow_public_ip
    selector         = local.lzy-service-labels
    ip_families      = var.lzy_service_ip_families
    port {
      port        = local.lzy-service-port
      target_port = local.lzy-service-port
    }
    type = "LoadBalancer"
  }
}

resource "kubernetes_service" "lzy_service_cluster_ip" {
  metadata {
    name        = "${local.lzy-service-k8s-name}-cluster-ip"
    labels      = local.lzy-service-labels
  }
  spec {
    selector         = local.lzy-service-labels
    port {
      port        = local.lzy-service-port
      target_port = local.lzy-service-port
    }
    type = "ClusterIP"
  }
}
