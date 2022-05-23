
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
            host_port      = 8123
          }
          volume_mount {
            mount_path = "/var/lib/clickhouse/"
            name       = "persistent"
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
        volume {
          name = "persistent"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.clickhouse_volume.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim" "clickhouse_volume" {
  metadata {
    name = "clickhouse-volume"
  }
  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = "64Gi"
      }
    }
  }
  wait_until_bound = false
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