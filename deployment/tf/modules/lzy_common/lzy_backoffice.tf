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

resource "kubernetes_pod" "lzy_backoffice" {
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
        container_port = 80
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
        container_port = 8080
      }
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
                "lzy-kharon",
                "lzy-backoffice"
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

  depends_on = [
    kubernetes_pod.lzy_server,
    helm_release.lzy_server_db
  ]
}

resource "kubernetes_service" "lzy_backoffice" {
  metadata {
    name = "lzy-backoffice-service"
    annotations = var.backoffice_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.backoffice_public_ip
    type             = "LoadBalancer"
    selector = {
      app : "lzy-backoffice"
    }
    port {
      name = "backend"
      port = 8080
    }
    port {
      name = "frontend"
      port = 80
    }
  }

  depends_on = [
    kubernetes_pod.lzy_backoffice
  ]
}
