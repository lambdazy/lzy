locals {
  whiteboard-labels = {
    app                         = "whiteboard"
    app.kubernetes.io / name    = "lzy-whiteboard"
    app.kubernetes.io / part-of = "lzy"
    lzy.ai / app                = "whiteboard"
  }
}

resource "kubernetes_secret" "whiteboard_db_data" {
  metadata {
    name   = "whiteboard-db-data"
    labels = local.whiteboard-labels
  }

  data = {
    password = var.lzy_whiteboard_db_password
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "whiteboard" {
  metadata {
    name   = "whiteboard"
    labels = local.whiteboard-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.whiteboard-labels
    }
    template {
      metadata {
        name   = "whiteboard"
        labels = local.whiteboard-labels
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
                  values   = local.all-services-k8s-app-labels
                }
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
}

resource "kubernetes_service" "whiteboard" {
  metadata {
    name   = "whiteboard"
    labels = local.whiteboard-labels
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
    selector = local.whiteboard-labels
    type     = "ClusterIP"
  }
}