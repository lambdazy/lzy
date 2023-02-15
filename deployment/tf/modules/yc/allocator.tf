resource "kubernetes_secret" "allocator_sa_key" {
  metadata {
    name = "allocator-sa-key"
  }
  data = {
    key = local.allocator-sa-key-json
  }
}

locals {
  allocator-sa-key-json = jsonencode({
    "id" : yandex_iam_service_account_key.allocator-sa-key.id
    "service_account_id" : yandex_iam_service_account_key.allocator-sa-key.service_account_id
    "created_at" : yandex_iam_service_account_key.allocator-sa-key.created_at
    "key_algorithm" : yandex_iam_service_account_key.allocator-sa-key.key_algorithm
    "public_key" : yandex_iam_service_account_key.allocator-sa-key.public_key
    "private_key" : yandex_iam_service_account_key.allocator-sa-key.private_key
  })

  user-clusters = [yandex_kubernetes_cluster.main.id]
  allocator-labels   = {
    app                         = "allocator"
    "app.kubernetes.io/name"    = "allocator"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "allocator"
  }
  allocator-k8s-name = "allocator"
  allocator-image = var.allocator-image
}

resource "kubernetes_stateful_set" "allocator" {
  metadata {
    name   = local.allocator-k8s-name
    labels = local.allocator-labels
  }
  spec {
    replicas = "1"
    selector {
      match_labels = local.allocator-labels
    }
    template {
      metadata {
        name   = local.allocator-k8s-name
        labels = local.allocator-labels
      }
      spec {
        container {
          name              = local.allocator-k8s-name
          image             = local.allocator-image
          image_pull_policy = "Always"
          port {
            container_port = local.allocator-port
            host_port      = local.allocator-port
          }
          port {
            container_port = local.allocator-metrics-port
            host_port = local.allocator-metrics-port
          }

          env {
            name = "ALLOCATOR_ADDRESS"
            value = "${kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]}:${local.allocator-port}"
          }

          env {
            name  = "ALLOCATOR_USER_CLUSTERS"
            value = yandex_kubernetes_cluster.allocator_cluster.id
          }

          env {
            name  = "ALLOCATOR_SERVICE_CLUSTERS"
            value = yandex_kubernetes_cluster.main.id
          }

          env {
            name  = "ALLOCATOR_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["allocator"].metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name  = "ALLOCATOR_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["allocator"].metadata[0].name
                key = "password"
              }
            }
          }

          env {
            name  = "ALLOCATOR_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/allocator"
          }

          env {
            name  = "ALLOCATOR_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name = "ALLOCATOR_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "ALLOCATOR_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name = "ALLOCATOR_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "key"
              }
            }
          }
          env {
            name  = "ALLOCATOR_YC_MK8S_ENABLED"
            value = "true"
          }
          env {
            name  = "ALLOCATOR_KUBER_NETWORK_POLICY_MANAGER_ENABLED"
            value = "true"
          }
          env {
            name  = "ALLOCATOR_YC_CREDENTIALS_SERVICE_ACCOUNT_FILE"
            value = "/tmp/sa-key/sa-key.json"
          }
          env {
            name  = "ALLOCATOR_YC_CREDENTIALS_ENDPOINT"
            value = var.yc-endpoint
          }
          env {
            name  = "ALLOCATOR_YC_CREDENTIALS_IAM_ENDPOINT"
            value = "iam.${var.yc-endpoint}"
          }
          env {
            name = "ALLOCATOR_HEARTBEAT_TIMEOUT"
            value = "5m"
          }
          env {
            name = "ALLOCATOR_ALLOCATION_TIMEOUT"
            value = "15m"
          }
          env {
            name = "ALLOCATOR_GC_CLEANUP_PERIOD"
            value = "1m"
          }
          env {
            name = "ALLOCATOR_GC_LEASE_DURATION"
            value = "30m"
          }

          env {
            name  = "ALLOCATOR_DISK_MANAGER_FOLDER_ID"
            value = var.folder_id
          }

          env {
            name  = "ALLOCATOR_DISK_MANAGER_DEFAULT_OPERATION_TIMEOUT"
            value = "5m"
          }

          env {
            name = "ALLOCATOR_INSTANCE_ID"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          volume_mount {
            name       = "sa-key"
            mount_path = "/tmp/sa-key/"
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
        }

        volume {
          name = "sa-key"
          secret {
            secret_name = kubernetes_secret.allocator_sa_key.metadata[0].name
            items {
              key  = "key"
              path = "sa-key.json"
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
        dns_policy    = "ClusterFirstWithHostNet"
        host_network  = true
      }
    }
    service_name = ""
  }
}

resource "kubernetes_service" "allocator_service" {
  metadata {
    name   = "${local.allocator-k8s-name}-load-balancer"
    labels = local.allocator-labels
    annotations = {
      "yandex.cloud/load-balancer-type": "internal"
      "yandex.cloud/subnet-id": yandex_vpc_subnet.custom-subnet.id
    }
  }
  spec {
    selector = local.allocator-labels
    port {
      port = local.allocator-port
      target_port = local.allocator-port
    }
    type = "LoadBalancer"
  }
}
