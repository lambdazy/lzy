locals {
  kharon-labels = {
    app                         = "lzy-kharon"
    app.kubernetes.io / name    = "lzy-kharon"
    app.kubernetes.io / part-of = "lzy"
    lzy.ai / app                = "kharon"
  }
}

resource "kubernetes_deployment" "kharon" {
  metadata {
    name   = "lzy-kharon"
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
        name   = "lzy-kharon"
        labels = local.kharon-labels
      }
      spec {
        container {
          name              = "lzy-kharon"
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
          port {
            container_port = 8899
            host_port      = 8899
          }
          port {
            container_port = 8900
            host_port      = 8900
          }
          args = [
            "--lzy-server-address",
            "http://$(LZY_SERVER_IP):8888",
            "--host",
            "$(LZY_HOST)",
            "-e",
            var.kharon_public_ip,
            "--port",
            "8899",
            "--servant-proxy-port",
            "8900",
            "--servantfs-proxy-port",
            "8901",
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
    name        = "lzy-kharon-load-balancer"
    labels      = local.kharon-labels
    annotations = var.kharon_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.kharon_public_ip
    type             = "LoadBalancer"
    selector         = local.kharon-labels
    port {
      port = 8899
    }
  }
}
