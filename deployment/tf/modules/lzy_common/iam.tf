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
  iam-internal-cred-type = "public_key"
}

resource "kubernetes_secret" "iam_db_data" {
  metadata {
    name      = "${local.iam-k8s-name}-db-data"
    labels    = local.iam-labels
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }

  data = {
    password = var.iam_db_password
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
            value = "jdbc:postgresql://${var.iam_db_host}:${var.iam_db_port}/${var.iam_db_name}"
          }
          env {
            name  = "IAM_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name  = "IAM_DATABASE_USERNAME"
            value = var.iam_db_user
          }
          env {
            name  = "IAM_DATABASE_PASSWORD"
            value = var.iam_db_password == "" ? random_password.iam_db_password[0].result : var.iam_db_password
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
            name  = "IAM_INTERNAL_USERNAME"
            value = local.iam-internal-user-name
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIALNAME"
            value = local.iam-internal-cred-name
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIALVAUE"
            value = tls_private_key.internal_key.public_key_pem
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIALTYPE"
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
                  key      = "app"
                  operator = "In"
                  values   = local.all-services-k8s-app-labels
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
  }
}
