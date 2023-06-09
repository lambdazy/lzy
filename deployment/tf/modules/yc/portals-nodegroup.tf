resource "kubernetes_namespace" "fictive_portals" {
  metadata {
    name = "fictive"
  }
}

resource "yandex_kubernetes_node_group" "portals" {
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "portals"
  description = "Nodegroup for lzy portals"
  node_labels = {
    "lzy.ai/logging_allowed" = "true"
    "lzy.ai/node-pool-id"    = "portals1"
    "lzy.ai/node-pool-label" = "portals"
    "lzy.ai/node-pool-kind"  = "CPU"
    "lzy.ai/node-pool-az"    = "ru-central1-a"
    "lzy.ai/node-pool-state" = "ACTIVE"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids = [yandex_vpc_subnet.custom-subnet.id]
      ipv4       = true
    }

    resources {
      memory = 4
      cores  = 4
    }

    boot_disk {
      type = "network-hdd"
      size = 64
    }

    scheduling_policy {
      preemptible = false
    }
  }

  scale_policy {
    fixed_scale {
      size = var.portals_pool_size
    }
  }
}

resource "kubernetes_daemonset" "portal_cpu_fictive_containers" {
  metadata {
    name      = "portals-fictive"
    namespace = kubernetes_namespace.fictive_portals.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "portal-image-caching"
      }
    }
    template {
      metadata {
        labels = {
          name = "portal-image-caching"
        }
      }
      spec {
        init_container {
          image             = var.portal_image
          image_pull_policy = "Always"
          name              = "fictive-portal"
          command           = ["sh", "-c", "exit 0"]
        }
        # Container for notifying allocator about node readiness
        container {
          image             = var.node-sync-image
          image_pull_policy = "Always"
          name              = "node-allocator-sync"
          command           = ["sh", "-c", "/entrypoint.sh $ALLOCATOR_IP; tail -f /dev/null"]

          env {
            name  = "CLUSTER_ID"
            value = yandex_kubernetes_cluster.main.id
          }
          env {
            name  = "NODE_NAME"
            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }
          env {
            name  = "ALLOCATOR_IP"
            value = kubernetes_service.allocator_service.status[0].load_balancer[0].ingress[0]["ip"]
          }
        }
        host_network = true
        node_selector = {
          "lzy.ai/node-pool-id" = "portals1"
        }
      }
    }
  }
}