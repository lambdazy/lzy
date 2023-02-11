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
    "lzy.ai/node-pool-id" = "portals1"
    "lzy.ai/node-pool-label" = "portals"
    "lzy.ai/node-pool-kind" = "CPU"
    "lzy.ai/node-pool-az" = "ru-central1-a"
    "lzy.ai/node-pool-state" = "ACTIVE"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids         = [yandex_vpc_subnet.custom-subnet.id]
      ipv4               = true
    }

    resources {
      memory = 2
      cores  = 2
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
      size = 10
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
        container {
          image   = var.portal_image
          name    = "fictive-portal"
          command = ["sleep", "10000000d"]
        }
        node_selector = {
          "lzy.ai/node-pool-id" = "portals1"
        }
      }
    }
  }
}