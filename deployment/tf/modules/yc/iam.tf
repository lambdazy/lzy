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
                name = kubernetes_secret.db_secret["iam"].metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "IAM_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["iam"].metadata[0].name
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
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/iam"
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

    annotations = {
      "yandex.cloud/load-balancer-type" : "internal"
      "yandex.cloud/subnet-id" : yandex_vpc_subnet.custom-subnet.id
    }
  }
  spec {
    selector = local.iam-labels
    port {
      port        = local.iam-port
      target_port = local.iam-port
    }
    type = "LoadBalancer"
  }
}
