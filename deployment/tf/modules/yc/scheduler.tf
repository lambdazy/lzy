locals {
  scheduler-labels   = {
    app                         = "scheduler"
    "app.kubernetes.io/name"    = "scheduler"
    "app.kubernetes.io/part-of" = "graph-executor"
    "lzy.ai/app"                = "scheduler"
  }
  scheduler-port     = 8765
  scheduler-k8s-name = "scheduler"
  scheduler-image = var.scheduler-image
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
            host_port      = local.scheduler-port
          }
          env {
            name  = "SCHEDULER_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["scheduler"].metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name  = "SCHEDULER_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["scheduler"].metadata[0].name
                key = "password"
              }
            }
          }

          env {
            name  = "SCHEDULER_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/scheduler"
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
            name  = "JOBS_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["scheduler"].metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name  = "JOBS_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["scheduler"].metadata[0].name
                key = "password"
              }
            }
          }

          env {
            name  = "JOBS_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/scheduler"
          }

          env {
            name  = "JOBS_DATABASE_ENABLED"
            value = "true"
          }

          env {
            name = "SCHEDULER_IAM_ADDRESS"
            value = "${kubernetes_service.iam.status[0].load_balancer[0].ingress[0]["ip"]}:${local.iam-port}"
          }
          env {
            name = "SCHEDULER_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name = "SCHEDULER_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "key"
              }
            }
          }

          env {
            name = "SCHEDULER_PORT"
            value = local.scheduler-port
          }

          env {
            name = "SCHEDULER_DEFAULT_PROVISIONING_LIMIT"
            value = "0"
          }

          env {
            name = "SCHEDULER_SCHEDULER_ADDRESS"
            value = "${kubernetes_service.scheduler_service.spec[0].cluster_ip}:${local.scheduler-port}"
          }

          env {
            name = "SCHEDULER_CHANNEL_MANAGER_ADDRESS"
            value = "${kubernetes_service.channel_manager_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.channel-manager-port}"
          }

          env {
            name = "SCHEDULER_ALLOCATOR_ADDRESS"
            value = "${kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.allocator-port}"
          }

          env {
            name = "SCHEDULER_WORKER_IMAGE"
            value = var.servant-image
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_ALLOCATION_TIMEOUT"
            value = "10m"
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_IDLE_TIMEOUT"
            value = "10m"
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_CONFIGURING_TIMEOUT"
            value = "10m"
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_WORKER_STOP_TIMEOUT"
            value = "1m"
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_EXECUTING_HEARTBEAT_PERIOD"
            value = "30s"
          }

          env {
            name = "SCHEDULER_WORKER_PROCESSOR_IDLE_HEARTBEAT_PERIOD"
            value = "5m"
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
        dns_policy    = "ClusterFirstWithHostNet"
        host_network  = true
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
      port = local.scheduler-port
      target_port = local.scheduler-port
    }
    type = "ClusterIP"
  }
}