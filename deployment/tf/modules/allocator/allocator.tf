resource "kubernetes_secret" "allocator_sa_key" {
  metadata {
    name = "allocator-sa-key"
  }
  data = {
    key = jsonencode({
      "id" : yandex_iam_service_account_key.allocator-sa-key.id
      "service_account_id" : yandex_iam_service_account_key.allocator-sa-key.service_account_id
      "created_at" : yandex_iam_service_account_key.allocator-sa-key.created_at
      "key_algorithm" : yandex_iam_service_account_key.allocator-sa-key.key_algorithm
      "public_key" : yandex_iam_service_account_key.allocator-sa-key.public_key
      "private_key" : yandex_iam_service_account_key.allocator-sa-key.private_key
    })
  }
}

locals {
  allocator-labels   = {
    app                         = "allocator"
    "app.kubernetes.io/name"    = "allocator"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "allocator"
  }
  allocator-port     = 10239
  allocator-k8s-name = "allocator"
}

resource "kubernetes_secret" "allocator_db_data" {
  metadata {
    name      = "${local.allocator-k8s-name}-db-data"
    labels    = local.allocator-labels
    namespace = "default"
  }

  data = {
    username = local.postgresql-configs.allocator.user,
    password = random_password.postgresql_db_passwords["allocator"].result
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "allocator" {
  metadata {
    name   = local.allocator-k8s-name
    labels = local.allocator-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
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
          image             = var.allocator-image
          image_pull_policy = "Always"
          port {
            container_port = local.allocator-port
            host_port      = local.allocator-port
          }
          env {
            name = "ALLOCATOR_HOST"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }
          env {
            name = "ALLOCATOR_ADDRESS"
            value = "${kubernetes_service.allocator_service.spec[0].cluster_ip}:${local.allocator-port}"
          }
          env {
            name  = "ALLOCATOR_USER_CLUSTERS"
            value = join(",", var.user-clusters)
          }
          env {
            name  = "ALLOCATOR_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/${local.postgresql-configs.allocator.db}"
          }
          env {
            name  = "ALLOCATOR_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.allocator_db_data.metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name  = "ALLOCATOR_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.allocator_db_data.metadata[0].name
                key = "password"
              }
            }
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
            value = local.iam-internal-user-name
          }
          env {
            name = "ALLOCATOR_IAM_INTERNAL_USER_PRIVATE_KEY"
            value = tls_private_key.internal_key.private_key_pem
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
            value = "api.cloud-preprod.yandex.net:443"
          }
          env {
            name  = "ALLOCATOR_YC_CREDENTIALS_IAM_ENDPOINT"
            value = "iam.api.cloud-preprod.yandex.net:443"
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
            name = "ALLOCATOR_GC_PERIOD"
            value = "1m"
          }
          volume_mount {
            name       = "sa-key"
            mount_path = "/tmp/sa-key/"
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
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_expressions {
                  key      = "app.kubernetes.io/managed-by"
                  operator = "In"
                  values   = ["Helm"]
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
  depends_on = [yandex_kubernetes_node_group.services]
}

resource "kubernetes_service" "allocator_service" {
  metadata {
    name   = "${local.allocator-k8s-name}-load-balancer"
    labels = local.allocator-labels
  }
  spec {
    selector = local.allocator-labels
    port {
      port = local.allocator-port
      target_port = local.allocator-port
    }
    type = "ClusterIP"
  }
}