resource "kubernetes_secret" "whiteboard_db_data" {
  metadata {
    name = "whiteboard-db-data"
  }

  data = {
    password = var.lzy_whiteboard_db_password
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "whiteboard" {
  metadata {
    name = "whiteboard"
    labels = {
      app = "whiteboard"
    }
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = {
        app = "whiteboard"
      }
    }
    template {
      metadata {
        name = "whiteboard"
        labels = {
          app = "whiteboard"
        }
      }
      spec {
        container {
          name              = "whiteboard"
          image             = var.whiteboard-image
          image_pull_policy = "Always"
          env {
            name  = "DATABASE_URL"
            value = "jdbc:postgresql://${var.lzy_whiteboard_db_host}:${var.lzy_whiteboard_db_port}/${var.lzy_whiteboard_db_name}"
          }
          env {
            name  = "DATABASE_USERNAME"
            value = var.lzy_whiteboard_db_user
          }
          env {
            name  = "DATABASE_PASSWORD"
            value = var.lzy_whiteboard_db_password == "" ? random_password.whiteboard_db_password[0].result : var.lzy_whiteboard_db_password
          }
          env {
            name  = "SERVER_URI"
            value = kubernetes_service.lzy_server.spec[0].cluster_ip
          }
          port {
            container_port = 8999
            host_port      = 8999
          }
          args = [
            "-z",
            "http://${kubernetes_service.lzy_server.spec[0].cluster_ip}:8888",
            "-p",
            "8999"
          ]
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

resource "kubernetes_service" "whiteboard" {
  metadata {
    name = "whiteboard"
    annotations = {
      #      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    port {
      protocol    = "TCP"
      port        = 8999
      target_port = 8999
    }
    selector = {
      app = "whiteboard"
    }
    type = "ClusterIP"
  }
}