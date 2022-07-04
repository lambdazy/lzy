locals {
  iam-labels = {
    app                         = "iam"
    "app.kubernetes.io/name"    = "iam"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "iam"
  }
  iam-port     = 8443
  iam-k8s-name = "iam"
}

resource "kubernetes_secret" "iam_db_data" {
  metadata {
    name      = "${local.iam-k8s-name}-db-data"
    labels    = local.iam-labels
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }

  data = {
    password = "-"
  }

  type = "Opaque"
}

resource "kubernetes_deployment" "iam" {
  metadata {
    name   = local.iam-k8s-name
    labels = local.iam-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.iam-labels
    }
    template {
      metadata {
        name   = local.iam-k8s-name
        labels = local.iam-labels
      }
      spec {
        container {
          name              = local.iam-k8s-name
          image             = var.iam-image
          image_pull_policy = "Always"
          port {
            container_port = local.iam-port
            host_port      = local.iam-port
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

resource "kubernetes_service" "iam" {
  metadata {
    name = "${local.iam-k8s-name}-service"
    labels = {
      labels = local.iam-labels
    }
  }
  spec {
    selector = local.iam-labels
    port {
      port        = local.iam-port
      target_port = local.iam-port
    }
  }
}
