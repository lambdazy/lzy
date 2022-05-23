resource "tls_private_key" "backoffice_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "kubernetes_secret" "backoffice_secrets" {
  metadata {
    name = "backoffice-secrets"
  }

  data = {
    private-key = tls_private_key.backoffice_key.private_key_pem
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "lzy_backoffice" {
  metadata {
    name = "lzy-backoffice"
    labels = {
      app = "lzy-backoffice"
    }
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = {
        app = "lzy-backoffice"
      }
    }
    template {
      metadata {
        name = "lzy-backoffice"
        labels = {
          app = "lzy-backoffice"
        }
      }
      spec {
        container {
          name              = "lzy-backoffice-frontend"
          image             = var.backoffice-frontend-image
          image_pull_policy = "Always"
          port {
            name           = "frontend"
            container_port = 80
            host_port      = 80
          }
          port {
            name           = "frontendtls"
            container_port = 443
            host_port      = 443
          }
          volume_mount {
            name       = "cert"
            mount_path = "/etc/sec"
          }
        }
        container {
          name              = "lzy-backoffice-backend"
          image             = var.backoffice-backend-image
          image_pull_policy = "Always"
          env {
            name  = "GRPC_HOST"
            value = kubernetes_service.lzy_server.spec[0].cluster_ip
          }
          env {
            name  = "GRPC_PORT"
            value = "8888"
          }
          env {
            name  = "GRPC_WBHOST"
            value = kubernetes_service.whiteboard.spec[0].cluster_ip
          }
          env {
            name  = "GRPC_WBPORT"
            value = "8999"
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
          volume_mount {
            name       = "sec"
            mount_path = "/etc/sec"
          }
          port {
            name           = "backend"
            container_port = 8080
            host_port      = 8080
          }
          port {
            name           = "backendtls"
            container_port = 8443
            host_port      = 8443
          }
          args = [
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
                  values = [
                    "lzy-servant",
                    "lzy-server",
                    "lzy-server-db",
                    "lzy-kharon",
                    "lzy-backoffice",
                    "whiteboard",
                    "whiteboard-db",
                    "grafana",
                    "kafka",
                    "clickhouse"
                  ]
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
    name        = "lzy-backoffice-service"
    annotations = var.backoffice_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.backoffice_public_ip
    type             = "LoadBalancer"
    selector = {
      app : "lzy-backoffice"
    }
    port {
      name        = "backend"
      port        = 8080
      target_port = 8080
    }
    dynamic "port" {
      for_each = var.ssl-enabled ? [1] : []
      content {
        name        = "backendtls"
        port        = 8443
        target_port = 8443
      }
    }
    dynamic "port" {
      for_each = var.ssl-enabled ? [1] : []
      content {
        name        = "frontendtls"
        port        = 443
        target_port = 443
      }
    }
    port {
      name        = "frontend"
      port        = 80
      target_port = 80
    }
  }
}
