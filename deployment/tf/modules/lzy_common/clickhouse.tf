
resource "kubernetes_deployment" "clickhouse" {
  metadata {
    name = "clickhouse"
    labels = {
      app = "clickhouse"
    }
  }
  spec {
    selector {
      match_labels = {
        app = "clickhouse"
      }
    }
    template {
      metadata {
        name = "clickhouse"
        labels = {
          app = "clickhouse"
        }
      }
      spec {
        container {
          name  = "clickhouse"
          image = var.clickhouse-image
          env {
            name = "CLICKHOUSE_USER"
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
            name  = "CLICKHOUSE_DB"
            value = "lzy"
          }
          env {
            name  = "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"
            value = "1"
          }
          port {
            container_port = 8123
          }
        }
        host_network = true
        dns_policy   = "ClusterFirstWithHostNet"
      }
    }
  }
}

resource "kubernetes_service" "clickhouse_service" {
  metadata {
    name = "clickhouse-service"
    annotations = {
      #      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    port {
      port        = 8123
      target_port = 8123
    }
    selector = {
      app = "clickhouse"
    }
  }
  depends_on = [
    kubernetes_deployment.clickhouse
  ]
}