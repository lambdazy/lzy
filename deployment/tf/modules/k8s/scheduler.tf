locals {
  scheduler-labels = {
    app                         = "scheduler"
    "app.kubernetes.io/name"    = "scheduler"
    "app.kubernetes.io/part-of" = "graph-executor"
    "lzy.ai/app"                = "scheduler"
  }
  scheduler-k8s-name = "scheduler"
  scheduler-image    = var.scheduler-image
}

resource "random_password" "scheduler_db_passwords" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "scheduler_db_secret" {
  metadata {
    name      = "db-secret-${local.scheduler-k8s-name}"
    namespace = "default"
  }

  data = {
    username = local.scheduler-k8s-name,
    password = random_password.scheduler_db_passwords.result,
    db_host  = var.db-host
    db_port  = 6432
    db_name  = local.scheduler-k8s-name
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "scheduler" {
  metadata {
    name   = local.scheduler-k8s-name
    labels = local.scheduler-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.scheduler-labels
    }
    template {
      metadata {
        name   = local.scheduler-k8s-name
        labels = local.scheduler-labels
      }
      spec {
        container {
          name              = local.scheduler-k8s-name
          image             = local.scheduler-image
          image_pull_policy = "Always"
          port {
            container_port = local.scheduler-port
          }
          port {
            container_port = local.scheduler-metrics-port
          }
          env {
            name  = "SCHEDULER_METRICS_PORT"
            value = local.scheduler-metrics-port
          }
          env {
            name = "SCHEDULER_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.scheduler_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "SCHEDULER_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.scheduler_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "SCHEDULER_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.scheduler_db_secret.data.db_host}:${kubernetes_secret.scheduler_db_secret.data.db_port}/${kubernetes_secret.scheduler_db_secret.data.db_name}"
          }

          env {
            name  = "SCHEDULER_DATABASE_ENABLED"
            value = "true"
          }

          env {
            name  = "SCHEDULER_USER_DEFAULT_IMAGE"
            value = "default"
          }

          env {
            name = "JOBS_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.scheduler_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "JOBS_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.scheduler_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "JOBS_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.scheduler_db_secret.data.db_host}:${kubernetes_secret.scheduler_db_secret.data.db_port}/${kubernetes_secret.scheduler_db_secret.data.db_name}"
          }

          env {
            name  = "JOBS_DATABASE_ENABLED"
            value = "true"
          }

          env {
            name  = "SCHEDULER_IAM_ADDRESS"
            value = "${kubernetes_service.iam.status[0].load_balancer[0].ingress[0]["ip"]}:${local.iam-port}"
          }
          env {
            name = "SCHEDULER_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "SCHEDULER_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }

          env {
            name  = "SCHEDULER_PORT"
            value = local.scheduler-port
          }

          env {
            name  = "SCHEDULER_DEFAULT_PROVISIONING_LIMIT"
            value = "0"
          }

          env {
            name  = "SCHEDULER_SCHEDULER_ADDRESS"
            value = "${kubernetes_service.scheduler_service.spec[0].cluster_ip}:${local.scheduler-port}"
          }

          env {
            name  = "SCHEDULER_CHANNEL_MANAGER_ADDRESS"
            value = "${kubernetes_service.channel_manager_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.channel-manager-port}"
          }

          env {
            name  = "SCHEDULER_ALLOCATOR_ADDRESS"
            value = "${kubernetes_service.allocator_service_cluster_ip.spec[0].cluster_ip}:${local.allocator-port}"
          }

          env {
            name  = "SCHEDULER_WORKER_IMAGE"
            value = var.servant-image
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_ALLOCATION_TIMEOUT"
            value = "10m"
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_IDLE_TIMEOUT"
            value = "10m"
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_CONFIGURING_TIMEOUT"
            value = "10m"
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_WORKER_STOP_TIMEOUT"
            value = "1m"
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_EXECUTING_HEARTBEAT_PERIOD"
            value = "30s"
          }

          env {
            name  = "SCHEDULER_WORKER_PROCESSOR_IDLE_HEARTBEAT_PERIOD"
            value = "5m"
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
            value = local.scheduler-k8s-name
          }

          dynamic "env" {
            for_each = local.kafka_env_map

            content {
              name  = "SCHEDULER_${env.key}"
              value = env.value
            }
          }

          dynamic "env" {
            for_each = var.enable_kafka ? [1] : []
            content {
              name  = "SCHEDULER_KAFKA_BOOTSTRAP_SERVERS"
              value = module.kafka[0].bootstrap-servers
            }
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
            name = kubernetes_config_map.unified-agent-config["scheduler"].metadata[0].name
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

resource "kubernetes_service" "scheduler_service" {
  metadata {
    name   = "${local.scheduler-k8s-name}-load-balancer"
    labels = local.scheduler-labels
  }
  spec {
    selector = local.scheduler-labels
    port {
      port        = local.scheduler-port
      target_port = local.scheduler-port
    }
    type = "ClusterIP"
  }
}