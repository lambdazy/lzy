locals {
  kharon-labels = {
    app                         = "lzy-kharon"
    "app.kubernetes.io/name"    = "lzy-kharon"
    "app.kubernetes.io/part-of" = "lzy"
    "lzy.ai/app"                = "kharon"
  }
  kharon-port                  = 8899
  kharon-servant-proxy-port    = 8900
  kharon-servant-fs-proxy-port = 8901
  kharon-k8s-name              = "lzy-kharon"
}

resource "kubernetes_deployment" "kharon" {
  metadata {
    name   = local.kharon-k8s-name
    labels = local.kharon-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }
    selector {
      match_labels = local.kharon-labels
    }
    template {
      metadata {
        name   = local.kharon-k8s-name
        labels = local.kharon-labels
      }
      spec {
        container {
          name              = local.kharon-k8s-name
          image             = var.kharon-image
          image_pull_policy = "Always"
          env {
            name = "LZY_HOST"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }
          env {
            name  = "LZY_SERVER_IP"
            value = kubernetes_service.lzy_server.spec[0].cluster_ip
          }
          env {
            name  = "KHARON_IAM_ADDRESS"
            value = "${kubernetes_service.iam.spec[0].cluster_ip}:${local.iam-port}"
          }
          port {
            container_port = local.kharon-port
            host_port      = local.kharon-port
          }
          port {
            container_port = local.kharon-servant-proxy-port
            host_port      = local.kharon-servant-proxy-port
          }
          port {
            container_port = local.kharon-servant-fs-proxy-port
            host_port      = local.kharon-servant-fs-proxy-port
          }
          args = [
            "--lzy-server-address",
            "http://$(LZY_SERVER_IP):8888",
            "--host",
            "$(LZY_HOST)",
            "-e",
            var.kharon_public_ip,
            "--port",
            local.kharon-port,
            "--servant-proxy-port",
            local.kharon-servant-proxy-port,
            "--servantfs-proxy-port",
            local.kharon-servant-fs-proxy-port,
            "-w",
            "http://${kubernetes_service.whiteboard.spec[0].cluster_ip}:8999",
            "-lsa",
            "http://${kubernetes_service.whiteboard.spec[0].cluster_ip}:8999"
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
}

resource "kubernetes_service" "lzy_kharon" {
  count = var.create_public_kharon_service ? 1 : 0
  metadata {
    name        = "${local.kharon-k8s-name}-load-balancer"
    labels      = local.kharon-labels
    annotations = var.kharon_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.kharon_public_ip
    type             = "LoadBalancer"
    selector         = local.kharon-labels
    port {
      port        = local.kharon-port
      target_port = local.kharon-port
    }
  }
}
