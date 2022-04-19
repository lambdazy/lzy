resource "kubernetes_deployment" "server" {
  metadata {
    name = "lzy-server"
    labels = {
      app = "lzy-server"
    }
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
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
        namespace = kubernetes_namespace.server_namespace.metadata[0].name
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
                name = kubernetes_secret.clickhouse_secret["server"].metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "CLICKHOUSE_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.clickhouse_secret["server"].metadata[0].name
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
            name  = "STORAGE_BUCKET"
            value = var.s3-bucket-name
          }
          dynamic "env" {
            for_each = var.storage-provider == "amazon" ? [1] : []
            content {
              name  = "STORAGE_AMAZON_ACCESS_TOKEN"
              value = var.amazon-access-key
            }
          }
          dynamic "env" {
            for_each = var.storage-provider == "amazon" ? [1] : []
            content {
              name  = "STORAGE_AMAZON_SECRET_TOKEN"
              value = var.amazon-secret-key
            }
          }
          dynamic "env" {
            for_each = var.storage-provider == "amazon" ? [1] : []
            content {
              name  = "STORAGE_AMAZON_ENDPOINT"
              value = var.amazon-service-endpoint
            }
          }
          dynamic "env" {
            for_each = var.storage-provider == "azure" ? [1] : []
            content {
              name  = "STORAGE_AZURE_CONNECTION_STRING"
              value = var.azure-connection-string
            }
          }
          dynamic "env" {
            for_each = var.storage-provider == "azure" ? [1] : []
            content {
              name  = "STORAGE_AZURE_ENABLED"
              value = "true"
            }
          }
          dynamic "env" {
            for_each = var.storage-provider == "amazon" ? [1] : []
            content {
              name  = "STORAGE_AMAZON_ENABLED"
              value = "true"
            }
          }
          env {
            name = "STORAGE_SEPARATED"
            value = var.s3-separated-per-bucket
          }
          env {
            name  = "SERVER_WHITEBOARD_URL"
            value = "http://${kubernetes_service.whiteboard.spec[0].cluster_ip}:8999"
          }
          env {
            name  = "SERVANT_IMAGE"
            value = var.servant-image
          }
          env {
            name  = "BASE_ENV_DEFAULT_IMAGE"
            value = var.base-env-default-image
          }
          env {
            name  = "KAFKA_LOGS_ENABLED"
            value = true
          }
          env {
            name = "KAFKA_LOGS_HOST"
            value = var.kafka-url
          }

          env {
            name = "DATABASE_ENABLED"
            value = "true"
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
          dynamic "env" {
            for_each = var.server-additional-envs
            iterator = data
            content {
              name = data.key
              value = data.value
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
  depends_on = [
    helm_release.lzy_server_db
  ]
}

resource "kubernetes_service" "lzy_server" {
  metadata {
    name = "lzy-server-service"
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
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
