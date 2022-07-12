locals {
  server-labels = {
    app                         = "lzy-server"
    "app.kubernetes.io/name"    = "lzy-server"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "server"
  }
  server-port     = 8888
  server-k8s-name = "lzy-server"
}

resource "kubernetes_secret" "lzy_server_db_data" {
  metadata {
    name      = "server-db-data"
    labels    = local.server-labels
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }

  data = {
    password = var.lzy_server_db_password
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "server" {
  metadata {
    name      = local.server-k8s-name
    labels    = local.server-labels
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.server-labels
    }
    template {
      metadata {
        name      = local.server-k8s-name
        labels    = local.server-labels
        namespace = kubernetes_namespace.server_namespace.metadata[0].name
      }
      spec {
        container {
          name              = local.server-k8s-name
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
            value = "jdbc:postgresql://${var.lzy_server_db_host}:${var.lzy_server_db_port}/${var.lzy_server_db_name}"
          }
          env {
            name  = "DATABASE_USERNAME"
            value = var.lzy_server_db_user
          }
          env {
            // TODO: replace with smth like LOGS_DB instead of CLICKHOUSE
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
            name  = "STORAGE_SEPARATED"
            value = var.s3-separated-per-bucket
          }
          env {
            name  = "SERVER_WHITEBOARD_URI"
            value = "http://${kubernetes_service.whiteboard.spec[0].cluster_ip}:${local.whiteboard-port}"
          }

          env {
            name  = "SERVER_IAM_URI"
            value = "http://${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
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

          env {
            name  = "SERVER_IAM_URI"
            value = "http://${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
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
            name  = "KAFKA_LOGS_HOST"
            value = var.kafka-url
          }

          env {
            name  = "DATABASE_ENABLED"
            value = "true"
          }

          env {
            name  = "DATABASE_PASSWORD"
            value = var.lzy_server_db_password == "" ? random_password.lzy_server_db_password[0].result : var.lzy_server_db_password
          }

          env {
            name  = "_JAVA_OPTIONS"
            value = "-Dserver.kuberAllocator.enabled=true"
          }

          dynamic "env" {
            for_each = var.server-additional-envs
            iterator = data
            content {
              name  = data.key
              value = data.value
            }
          }
          port {
            container_port = local.server-port
            host_port      = local.server-port
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
  depends_on = [
    helm_release.lzy_server_db
  ]
}

resource "kubernetes_service" "lzy_server" {
  metadata {
    name      = "${local.server-k8s-name}-service"
    labels    = local.server-labels
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
    annotations = {
      #      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    selector = local.server-labels
    type     = "ClusterIP"
    port {
      port        = local.server-port
      target_port = local.server-port
    }
  }
}
