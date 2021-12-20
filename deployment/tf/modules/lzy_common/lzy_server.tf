resource "kubernetes_deployment" "server" {
  metadata {
    name = "lzy-server"
    labels = {
      app = "lzy-server"
    }
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = {
        app = "lzy-server"
      }
    }
    template {
      metadata {
        name = "lzy-server"
        labels = {
          app = "lzy-server"
        }
      }
      spec {
        container {
          name              = "lzy-server"
          image             = var.server-image
          image_pull_policy = "Always"
          env {
            name = "LZY_SERVER_HOST"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }
          env {
            name  = "AUTHENTICATOR"
            value = "DbAuthenticator"
          }
          env {
            name  = "DATABASE_URL"
            value = "jdbc:postgresql://postgres-postgresql.default.svc.cluster.local:5432/serverDB"
          }
          env {
            name  = "DATABASE_USERNAME"
            value = "server"
          }
          env {
            name  = "CLICKHOUSE_ENABLED"
            value = "true"
          }
          env {
            name = "CLICKHOUSE_USERNAME"
            value_from {
              secret_key_ref {
                name = "clickhouse"
                key  = "username"
              }
            }
          }
          env {
            name = "CLICKHOUSE_PASSWORD"
            value_from {
              secret_key_ref {
                name = "clickhouse"
                key  = "password"
              }
            }
          }
          env {
            name  = "CLICKHOUSE_URL"
            value = "jdbc:clickhouse://clickhouse-service.default.svc.cluster.local:8123/lzy"
          }
          env {
            name  = "AGENTS_NAMES"
            value = "backoffice"
          }
          env {
            name  = "AGENTS_PUBLIC_KEYS"
            value = tls_private_key.backoffice_key.public_key_pem
          }
          env {
            name  = "AGENTS_ROLES"
            value = "backoffice"
          }
          env {
            name = "BUCKET_NAME"
            value = var.s3-bucket-name
          }
          env {
            name = "ACCESS_KEY"
            value = var.s3-access-key
          }
          env {
            name = "SECRET_KEY"
            value = var.s3-secret-key
          }
          env {
            name = "REGION"
            value = "us-west-2"
          }
          env {
            name = "SERVICE_ENDPOINT"
            value = var.s3-service-endpoint
          }
          env {
            name = "USE_S3_PROXY"
            value = var.s3-use-proxy
          }
          env {
            name = "S3_PROXY_PROVIDER"
            value = var.s3-proxy-provider
          }
          env {
            name = "S3_PROXY_IDENTITY"
            value = var.s3-access-key
          }
          env {
            name = "S3_PROXY_CREDENTIALS"
            value = var.s3-secret-key
          }
          env {
            name = "LZYWHITEBOARD"
            value = "http://${kubernetes_service.whiteboard.spec[0].cluster_ip}:8999"
          }

          env {
            name = "DATABASE_PASSWORD"
            value_from {
              secret_key_ref {
                name = "postgres"
                key  = "postgresql-password"
              }
            }
          }
          port {
            container_port = 8888
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
                    "lzy-backoffice",
                    "whiteboard"
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
  depends_on = [
    helm_release.lzy_server_db
  ]
}

resource "kubernetes_service" "lzy_server" {
  metadata {
    name = "lzy-server-service"
    annotations = {
      #      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    port {
      port        = 8888
      target_port = 8888
    }
    selector = {
      app = "lzy-server"
    }
  }
}
