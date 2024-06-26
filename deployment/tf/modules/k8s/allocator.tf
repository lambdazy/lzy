locals {
  allocator-labels = {
    app                         = "allocator"
    "app.kubernetes.io/name"    = "allocator"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "allocator"
  }
  allocator-k8s-name = "allocator"
  allocator-image    = var.allocator-image

  cidrs_list    = [for c in var.allocator_service_cidrs : "{\"cidr\": \"${c}\", \"ports\": [9876, 9877, 9878]}"] //TODO use scheduler properties
  cidrs = "[${join(", ", local.cidrs_list)}]"
}

resource "random_password" "allocator_db_passwords" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "allocator_db_secret" {
  metadata {
    name      = "db-secret-${local.allocator-k8s-name}"
    namespace = "default"
  }

  data = {
    username = local.allocator-k8s-name,
    password = random_password.allocator_db_passwords.result,
    db_host  = var.db-host
    db_port  = 6432
    db_name  = local.allocator-k8s-name
  }

  type = "Opaque"
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
          }
          port {
            container_port = local.allocator-metrics-port
          }

          env {
            name  = "MICRONAUT_SERVER_HOST"
            value = "0.0.0.0"
          }

          env {
            name  = "ALLOCATOR_METRICS_PORT"
            value = local.allocator-metrics-port
          }

          env {
            name  = "ALLOCATOR_HOSTS"
            value = join(",", [kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]])
          }

          env {
            name  = "ALLOCATOR_PORT"
            value = local.allocator-port
          }

          env {
            name  = "ALLOCATOR_USER_CLUSTERS"
            value = var.pool-cluster-id
          }

          env {
            name  = "ALLOCATOR_SERVICE_CLUSTERS"
            value = var.service-cluster-id
          }

          env {
            name = "ALLOCATOR_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.allocator_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "ALLOCATOR_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.allocator_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "ALLOCATOR_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.allocator_db_secret.data.db_host}:${kubernetes_secret.allocator_db_secret.data.db_port}/${kubernetes_secret.allocator_db_secret.data.db_name}"
          }

          env {
            name  = "ALLOCATOR_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "ALLOCATOR_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "ALLOCATOR_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "ALLOCATOR_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }
          env {
            name  = "ALLOCATOR_YC_MK8S_ENABLED"
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
            name  = "ALLOCATOR_HEARTBEAT_TIMEOUT"
            value = "5m"
          }
          env {
            name  = "ALLOCATOR_ALLOCATION_TIMEOUT"
            value = "15m"
          }
          env {
            name  = "ALLOCATOR_GC_CLEANUP_PERIOD"
            value = "1m"
          }
          env {
            name  = "ALLOCATOR_GC_LEASE_DURATION"
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
          env {
            name  = "ALLOCATOR_POLICY_ENABLED"
            value = "true"
          }
          env {
            name  = "ALLOCATOR_SERVICE_CIDRS"
            value = local.cidrs
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
            value = local.allocator-k8s-name
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
            name = kubernetes_config_map.unified-agent-config["allocator"].metadata[0].name
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
    service_name = ""
  }
}

resource "kubernetes_service" "allocator_service" {
  metadata {
    name        = "${local.allocator-k8s-name}-load-balancer"
    labels      = local.allocator-labels
    annotations = {
      "yandex.cloud/load-balancer-type" : "internal"
      "yandex.cloud/subnet-id" : var.custom-subnet-id
    }
  }
  spec {
    selector    = local.allocator-labels
    ip_families = var.allocator_service_ip_families
    port {
      name        = "main-grpc"
      port        = local.allocator-port
      target_port = local.allocator-port
    }
    port {
      name        = "http"
      port        = local.allocator-http-port
      target_port = local.allocator-http-port
    }
    external_traffic_policy = "Local"
    type                    = "LoadBalancer"
  }
}

resource "kubernetes_service" "allocator_service_cluster_ip" {
  metadata {
    name   = "${local.allocator-k8s-name}-cluster-ip"
    labels = local.allocator-labels
  }
  spec {
    selector = local.allocator-labels
    port {
      name        = "main-grpc"
      port        = local.allocator-port
      target_port = local.allocator-port
    }
    port {
      name        = "http"
      port        = local.allocator-http-port
      target_port = local.allocator-http-port
    }
    type = "ClusterIP"
  }
}
