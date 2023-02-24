

locals {
  whiteboard-labels   = {
    app                         = "whiteboard"
    "app.kubernetes.io/name"    = "whiteboard"
    "lzy.ai/app"                = "whiteboard"
  }
  whiteboard-k8s-name = "whiteboard"
  whiteboard-image = var.whiteboard-image
}

resource "kubernetes_deployment" "whiteboard" {
  metadata {
    name   = local.whiteboard-k8s-name
    labels = local.whiteboard-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.whiteboard-labels
    }
    template {
      metadata {
        name   = local.whiteboard-k8s-name
        labels = local.whiteboard-labels
      }
      spec {
        container {
          name              = local.whiteboard-k8s-name
          image             = local.whiteboard-image
          image_pull_policy = "Always"
          port {
            container_port = local.whiteboard-port
          }

          env {
            name = "WHITEBOARD_ADDRESS"
            value = "0.0.0.0:${local.whiteboard-port}"
          }

          env {
            name  = "WHITEBOARD_DATABASE_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["whiteboard"].metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name  = "WHITEBOARD_DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_secret["whiteboard"].metadata[0].name
                key = "password"
              }
            }
          }

          env {
            name  = "WHITEBOARD_DATABASE_URL"
            value = "jdbc:postgresql://${yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn}:6432/whiteboard"
          }

          env {
            name  = "WHITEBOARD_DATABASE_ENABLED"
            value = "true"
          }
          env {
            name = "WHITEBOARD_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "WHITEBOARD_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name = "WHITEBOARD_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "key"
              }
            }
          }
        }
        node_selector = {
          type = "lzy"
        }
      }
    }
  }
}

resource "kubernetes_service" "whiteboard_service" {
  metadata {
    name   = "${local.whiteboard-k8s-name}-load-balancer"
    labels = local.whiteboard-labels
  }
  spec {
    selector = local.whiteboard-labels
    load_balancer_ip = yandex_vpc_address.whiteboard_public_ip.external_ipv4_address[0].address
    port {
      port = local.whiteboard-port
      target_port = local.whiteboard-port
    }
    type = "LoadBalancer"
  }
}