resource "kubernetes_namespace" "fictive" {
  metadata {
    name = "fictive"
  }
}

resource "yandex_kubernetes_node_group" "workers-s" {
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "workers-s"
  description = "Nodegroup for lzy workers with label s"
  node_labels = {
    "lzy.ai/node-pool-id" = "s1"
    "lzy.ai/node-pool-label" = "s"
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
    auto_scale {
      initial = 2
      max     = 10
      min     = 2
    }
  }
}

resource "kubernetes_daemonset" "worker_cpu_fictive_containers" {
  metadata {
    name      = "worker-fictive-containers-cpu"
    namespace = kubernetes_namespace.fictive.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "worker-image-caching"
      }
    }
    template {
      metadata {
        labels = {
          name = "worker-image-caching"
        }
      }
      spec {
        container {
          image   = var.servant-image
          name    = "fictive-worker"
          command = ["tail", "-f", "/entrypoint.sh"]
        }
        node_selector = {
          "lzy.ai/node-pool-id" = "s1"
        }
      }
    }
  }
}