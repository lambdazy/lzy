locals {
  whiteboard-labels = {
    app                         = "whiteboard"
    "app.kubernetes.io/name"    = "lzy-whiteboard"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "whiteboard"
  }
  whiteboard-port     = 8999
  whiteboard-k8s-name = "whiteboard"
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
    name   = local.whiteboard-k8s-name
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
        name   = local.whiteboard-k8s-name
        labels = local.whiteboard-labels
      }
      spec {
        container {
          name              = local.whiteboard-k8s-name
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
            container_port = local.whiteboard-port
            host_port      = local.whiteboard-port
          }
          args = [
            "-z",
            "http://${kubernetes_service.lzy_server.spec[0].cluster_ip}:${local.server-port}",
            "-p",
            local.whiteboard-port
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
  depends_on = [
    helm_release.whiteboard_db
  ]
}

resource "kubernetes_service" "whiteboard" {
  metadata {
    name   = local.whiteboard-k8s-name
    labels = local.whiteboard-labels
    annotations = {
      #      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    selector = local.whiteboard-labels
    type     = "ClusterIP"
    port {
      protocol    = "TCP"
      port        = local.whiteboard-port
      target_port = local.whiteboard-port
    }
  }
}