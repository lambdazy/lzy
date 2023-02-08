locals {
  backoffice-labels = {
    app                         = "lzy-backoffice"
    "app.kubernetes.io/name"    = "lzy-backoffice"
    "lzy.ai/app"                = "backoffice"
  }
  backoffice-frontend-port     = 80
  backoffice-frontend-tls-port = 443
  backoffice-backend-port      = 8080
  backoffice-backend-tls-port  = 8443
  backoffice-k8s-name          = "lzy-backoffice"
}

resource "kubernetes_secret" "oauth_github" {
  metadata {
    name = "oauth-github"
  }

  data = {
    client-id     = var.oauth-github-client-id
    client-secret = var.oauth-github-client-secret
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
          dynamic "volume_mount" {
            for_each = var.ssl-enabled ? [1] : []
            content {
              name       = "cert"
              mount_path = "/etc/sec"
            }
          }
        }
      container {
          name              = "${local.backoffice-k8s-name}-backend"
          image             = var.backoffice-backend-image
          image_pull_policy = "Always"
          env {
            name = "SITE_GITHUB_CREDENTIALS_CLIENT_ID"
            value_from {
              secret_key_ref {
                name = "oauth-github"
                key  = "client-id"
              }
            }
          }
          env {
            name = "SITE_GITHUB_CREDENTIALS_CLIENT_SECRET"
            value_from {
              secret_key_ref {
                name = "oauth-github"
                key  = "client-secret"
              }
            }
          }
          env {
            name = "SITE_SCHEDULER_ADDRESS"
            value = "${kubernetes_service.scheduler_service.spec[0].cluster_ip}:${local.scheduler-port}"
          }
          env {
            name = "SITE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "SITE_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "username"
              }
            }
          }
          env {
            name = "SITE_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key = "key"
              }
            }
          }
          env {
            name = "SITE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
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

          args = var.ssl-enabled ? [
            "-Dmicronaut.env.deduction=true",
            "-Dmicronaut.ssl.key-store.password=${var.ssl-keystore-password}",
            "-Dmicronaut.ssl.key-store.type=JKS",
            "-Dmicronaut.ssl.enabled=true",
            "-Dsite.hostname=https://${yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address}:8443",
            "-Dmicronaut.server.dual-protocol=true"
          ] : [
            "-Dmicronaut.env.deduction=true",
            "-Dmicronaut.ssl.enabled=false",
            "-Dsite.hostname=http://${yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address}:8080",
            "-Dmicronaut.server.dual-protocol=false"
          ]
        }

        dynamic "volume" {
          for_each = var.ssl-enabled ? [1] : []
          content {
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
        }

        node_selector = {
          type = "lzy"
        }
        host_network = true
        dns_policy   = "ClusterFirstWithHostNet"
      }
    }
  }
}


resource "kubernetes_service" "lzy_backoffice" {
  metadata {
    name        = "${local.backoffice-k8s-name}-service"
    labels      = local.backoffice-labels
  }
  spec {
    load_balancer_ip = yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address
    type             = "LoadBalancer"
    selector         = local.backoffice-labels
    port {
      name        = "backend"
      port        = local.backoffice-backend-port
      target_port = local.backoffice-backend-port
    }
    port {
      name        = "backendtls"
      port        = local.backoffice-backend-tls-port
      target_port = local.backoffice-backend-tls-port
    }
    port {
      name        = "frontendtls"
      port        = local.backoffice-frontend-tls-port
      target_port = local.backoffice-frontend-tls-port
    }
    port {
      name        = "frontend"
      port        = local.backoffice-frontend-port
      target_port = local.backoffice-frontend-port
    }
  }
}