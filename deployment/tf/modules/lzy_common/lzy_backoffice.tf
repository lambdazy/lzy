locals {
  backoffice-labels = {
    app                         = "lzy-backoffice"
    "app.kubernetes.io/name"    = "lzy-backoffice"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "backoffice"
    "lzy.ai/role"               = "system"
  }
  backoffice-frontend-port     = 80
  backoffice-frontend-tls-port = 443
  backoffice-backend-port      = 8080
  backoffice-backend-tls-port  = 8443
  backoffice-k8s-name          = "lzy-backoffice"
}

resource "tls_private_key" "backoffice_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "kubernetes_secret" "backoffice_secrets" {
  metadata {
    labels = local.backoffice-labels
    name   = "backoffice-secrets"
  }

  data = {
    private-key = tls_private_key.backoffice_key.private_key_pem
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "lzy_backoffice" {
  metadata {
    name   = local.backoffice-k8s-name
    labels = local.backoffice-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.backoffice-labels
    }
    template {
      metadata {
        name   = local.backoffice-k8s-name
        labels = local.backoffice-labels
      }
      spec {
        container {
          name              = "${local.backoffice-k8s-name}-frontend"
          image             = var.backoffice-frontend-image
          image_pull_policy = "Always"
          port {
            name           = "frontend"
            container_port = local.backoffice-frontend-port
            host_port      = local.backoffice-frontend-port
          }
          port {
            name           = "frontendtls"
            container_port = local.backoffice-frontend-tls-port
            host_port      = local.backoffice-frontend-tls-port
          }
          volume_mount {
            name       = "cert"
            mount_path = "/etc/sec"
          }
        }
        container {
          name              = "${local.backoffice-k8s-name}-backend"
          image             = var.backoffice-backend-image
          image_pull_policy = "Always"
          env {
            name  = "GRPC_HOST"
            value = kubernetes_service.lzy_server.spec[0].cluster_ip
          }
          env {
            name  = "GRPC_PORT"
            value = local.server-port
          }
          env {
            name  = "GRPC_WBHOST"
            value = kubernetes_service.whiteboard.spec[0].cluster_ip
          }
          env {
            name  = "GRPC_WBPORT"
            value = local.whiteboard-port
          }
          env {
            name = "OAUTH_GITHUB_CLIENT_ID"
            value_from {
              secret_key_ref {
                name = "oauth-github"
                key  = "client-id"
              }
            }
          }
          env {
            name  = "CREDENTIALS_USER_ID"
            value = "backoffice"
          }
          env {
            name = "OAUTH_GITHUB_CLIENT_SECRET"
            value_from {
              secret_key_ref {
                name = "oauth-github"
                key  = "client-secret"
              }
            }
          }
          env {
            name  = "CREDENTIALS_PRIVATE_KEY_PATH"
            value = "/etc/sec/backofficePrivateKey.txt"
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
            name  = "IAM_INTERNAL_CREDENTIAL_PRIVATE_KEY"
            value = tls_private_key.internal_key.private_key_pem
          }
          env {
            name  = "IAM_INTERNAL_CREDENTIAL_TYPE"
            value = local.iam-internal-cred-type
          }
          volume_mount {
            name       = "sec"
            mount_path = "/etc/sec"
          }
          port {
            name           = "backend"
            container_port = local.backoffice-backend-port
            host_port      = local.backoffice-backend-port
          }
          port {
            name           = "backendtls"
            container_port = local.backoffice-backend-tls-port
            host_port      = local.backoffice-backend-tls-port
          }
          args = [
            "-Dmicronaut.env.deduction=true",
            "-Dmicronaut.ssl.keyStore.password=${var.ssl-keystore-password}",
            "-Dmicronaut.ssl.enabled=${var.ssl-enabled ? "true" : "false"}",
            "-Dmicronaut.server.dual-protocol=${var.ssl-enabled ? "true" : "false"}"
          ]
        }
        volume {
          name = "sec"
          secret {
            secret_name = "backoffice-secrets"
            items {
              key  = "private-key"
              path = "backofficePrivateKey.txt"
            }
          }
        }
        volume {
          name = "cert"
          secret {
            secret_name = "certs"
            items {
              key  = "cert"
              path = "cert.crt"
            }
            items {
              key  = "cert-key"
              path = "cert.key"
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

resource "kubernetes_service" "lzy_backoffice" {
  count = var.create_public_backoffice_service ? 1 : 0
  metadata {
    name        = "${local.backoffice-k8s-name}-service"
    labels      = local.backoffice-labels
    annotations = var.backoffice_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.backoffice_public_ip
    type             = "LoadBalancer"
    selector         = local.backoffice-labels
    port {
      name        = "backend"
      port        = local.backoffice-backend-port
      target_port = local.backoffice-backend-port
    }
    dynamic "port" {
      for_each = var.ssl-enabled ? [1] : []
      content {
        name        = "backendtls"
        port        = local.backoffice-backend-tls-port
        target_port = local.backoffice-backend-tls-port
      }
    }
    dynamic "port" {
      for_each = var.ssl-enabled ? [1] : []
      content {
        name        = "frontendtls"
        port        = local.backoffice-frontend-tls-port
        target_port = local.backoffice-frontend-tls-port
      }
    }
    port {
      name        = "frontend"
      port        = local.backoffice-frontend-port
      target_port = local.backoffice-frontend-port
    }
  }
}
