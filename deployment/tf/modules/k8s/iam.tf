locals {
  iam-labels = {
    app                         = "iam"
    "app.kubernetes.io/name"    = "iam"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "iam"
  }
  iam-k8s-name           = "iam"
  iam-internal-user-name = "lzy-internal-agent"
  iam-internal-cred-name = "Internal agent key"
  iam-internal-cred-type = "PUBLIC_KEY"
  iam-docker-image       = var.iam-image
}

resource "kubernetes_secret" "iam_internal_user_data" {
  metadata {
    name   = "${local.iam-k8s-name}-user-data"
    labels = local.iam-labels
  }

  data = {
    username = local.iam-internal-user-name
    key      = tls_private_key.internal_key.private_key_pem
  }

  type = "Opaque"
}

resource "tls_private_key" "internal_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "random_password" "iam_db_passwords" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "iam_db_secret" {
  metadata {
    name      = "db-secret-${local.iam-k8s-name}"
    namespace = "default"
  }

  data = {
    username = local.iam-k8s-name,
    password = random_password.iam_db_passwords.result,
    db_host  = var.db-host
    db_port  = 6432
    db_name  = local.iam-k8s-name
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "iam" {
  metadata {
    name   = local.iam-k8s-name
    labels = local.iam-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.iam-labels
    }
    template {
      metadata {
        name   = local.iam-k8s-name
        labels = local.iam-labels
      }
      spec {
        container {
          name              = local.iam-k8s-name
          image             = local.iam-docker-image
          image_pull_policy = "Always"

          env {
            name = "IAM_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_db_secret.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "IAM_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_db_secret.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "IAM_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "IAM_DATABASE_URL"
            value = "jdbc:postgresql://${kubernetes_secret.iam_db_secret.data.db_host}:${kubernetes_secret.iam_db_secret.data.db_port}/${kubernetes_secret.iam_db_secret.data.db_name}"
          }

          env {
            name  = "IAM_USER_LIMIT"
            value = 60
          }
          env {
            name  = "IAM_SERVER_PORT"
            value = local.iam-port
          }
          env {
            name  = "IAM_INTERNAL_USER_NAME"
            value = local.iam-internal-user-name
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIAL_NAME"
            value = local.iam-internal-cred-name
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIAL_VALUE"
            value = tls_private_key.internal_key.public_key_pem
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIAL_TYPE"
            value = local.iam-internal-cred-type
          }
          env {
            name  = "IAM_METRICS_PORT"
            value = local.iam-metrics-port
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
            value = local.iam-k8s-name
          }
          port {
            container_port = local.iam-port
          }
          port {
            container_port = local.iam-metrics-port
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
            name = kubernetes_config_map.unified-agent-config["iam"].metadata[0].name
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

resource "kubernetes_service" "iam" {
  metadata {
    name   = "${local.iam-k8s-name}-service"
    labels = local.iam-labels

    annotations = {
      "yandex.cloud/load-balancer-type" : "internal"
      "yandex.cloud/subnet-id" : var.custom-subnet-id
    }
  }
  spec {
    selector    = local.iam-labels
    ip_families = var.iam_ip_families
    port {
      port        = local.iam-port
      target_port = local.iam-port
    }
    type = "LoadBalancer"
  }
}
