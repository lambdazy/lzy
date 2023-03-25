locals {
  backoffice-labels = {
    app                      = "lzy-backoffice"
    "app.kubernetes.io/name" = "lzy-backoffice"
    "lzy.ai/app"             = "backoffice"
  }

  backoffice-k8s-name     = "lzy-backoffice"
  github-redirect-address = var.domain_name != null ? var.domain_name : yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address
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
          }
          port {
            name           = "frontendtls"
            container_port = local.backoffice-frontend-tls-port
          }
          dynamic "volume_mount" {
            for_each = var.ssl-enabled ? [1] : []
            content {
              name       = "cert"
              mount_path = "/etc/sec"
            }
          }
          dynamic "volume_mount" {
            for_each = var.ssl-enabled ? [1] : []
            content {
              name       = "nginx-config"
              mount_path = "/etc/nginx/nginx.conf"
              sub_path   = "nginx.conf"
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
            name  = "SITE_SCHEDULER_ADDRESS"
            value = "${kubernetes_service.scheduler_service.spec[0].cluster_ip}:${local.scheduler-port}"
          }
          env {
            name  = "SITE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name = "SITE_IAM_INTERNAL_USER_NAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "SITE_IAM_INTERNAL_USER_PRIVATE_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.iam_internal_user_data.metadata[0].name
                key  = "key"
              }
            }
          }
          env {
            name  = "SITE_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          env {
            name  = "SITE_METRICS_PORT"
            value = local.site-metrics-port
          }
          port {
            name           = "backend"
            container_port = local.backoffice-backend-port
          }
          port {
            name           = "backendtls"
            container_port = local.backoffice-backend-tls-port
          }
          port {
            container_port = local.site-metrics-port
          }

          args = var.ssl-enabled ? [
            "-Dmicronaut.env.deduction=true",
            "-Dmicronaut.ssl.key-store.password=${var.ssl-keystore-password}",
            "-Dmicronaut.ssl.key-store.path=file:/app/keystore/keystore.jks",
            "-Dmicronaut.ssl.key-store.type=JKS",
            "-Dmicronaut.ssl.enabled=true",
            "-Dsite.hostname=https://${local.github-redirect-address}:8443",
            "-Dmicronaut.server.dual-protocol=true"
            ] : [
            "-Dmicronaut.env.deduction=true",
            "-Dmicronaut.ssl.enabled=false",
            "-Dsite.hostname=http://${yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address}:8080",
            "-Dmicronaut.server.dual-protocol=false"
          ]

          dynamic "volume_mount" {
            for_each = var.ssl-enabled ? [1] : []
            content {
              name       = "keystore"
              mount_path = "/app/keystore"
            }
          }
          volume_mount {
            name       = "varloglzy"
            mount_path = "/var/log/lzy"
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
          volume_mount {
            name       = "unified-agent-config"
            mount_path = "/etc/yandex/unified_agent/conf.d/"
          }
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

        dynamic "volume" {
          for_each = var.ssl-enabled ? [1] : []
          content {
            name = "keystore"
            secret {
              secret_name = "keystore"
              items {
                key  = "keystore"
                path = "keystore.jks"
              }
            }
          }
        }
        dynamic "volume" {
          for_each = var.ssl-enabled ? [1] : []
          content {
            name = "nginx-config"
            config_map {
              name = kubernetes_config_map.frontend-nginx-ssl-config[0].metadata[0].name
              items {
                key  = "config"
                path = "nginx.conf"
              }
            }
          }
        }
        volume {
          name = "unified-agent-config"
          config_map {
            name = kubernetes_config_map.unified-agent-config["site"].metadata[0].name
            items {
              key = "config"
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
      }
    }
  }
}


resource "kubernetes_service" "lzy_backoffice" {
  metadata {
    name   = "${local.backoffice-k8s-name}-service"
    labels = local.backoffice-labels
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