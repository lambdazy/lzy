locals {
  iam-labels = {
    app                         = "iam"
    "app.kubernetes.io/name"    = "iam"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "iam"
  }
  iam-port     = 8443
  iam-k8s-name = "iam"
  iam-internal-user-name = "lzy-internal-agent"
  iam-internal-cred-name = "Internal agent key"
  iam-internal-cred-type = "PUBLIC_KEY"
}

resource "kubernetes_secret" "iam_db_data" {
  metadata {
    name      = "${local.iam-k8s-name}-db-data"
    labels    = local.iam-labels
    namespace = "default"
  }

  data = {
    password = random_password.postgresql_db_passwords["iam"].result
  }

  type = "Opaque"
}

resource "tls_private_key" "internal_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
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
          image             = var.iam-image
          image_pull_policy = "Always"
          env {
            name  = "IAM_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/${local.postgresql-configs.iam.db}"
          }
          env {
            name  = "IAM_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "IAM_DATABASE_USERNAME"
            value = local.postgresql-configs.iam.user
          }
          env {
            name  = "IAM_DATABASE_PASSWORD"
            value = random_password.postgresql_db_passwords["iam"].result
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
          port {
            container_port = local.iam-port
            host_port      = local.iam-port
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
        host_network = true
        dns_policy   = "ClusterFirstWithHostNet"
      }
    }
  }
  depends_on = [yandex_kubernetes_node_group.services]
}

resource "kubernetes_service" "iam" {
  metadata {
    name   = "${local.iam-k8s-name}-service"
    labels = local.iam-labels
  }
  spec {
    selector = local.iam-labels
    port {
      port        = local.iam-port
      target_port = local.iam-port
    }
    type = "ClusterIP"
  }
}
