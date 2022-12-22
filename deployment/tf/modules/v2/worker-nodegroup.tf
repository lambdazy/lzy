resource "kubernetes_namespace" "fictive" {
  metadata {
    name = "fictive"
  }
  provider = kubernetes.allocator
}

resource "yandex_kubernetes_cluster" "allocator_cluster" {
  name        = "allocator-cluster"
  description = "Allocator k8s cluster"
  release_channel = "RAPID"

  network_id              = var.network_id
  cluster_ipv4_range      = "10.21.0.0/16"
  service_ipv4_range      = "10.22.0.0/16"
  master {
    zonal {
      zone      = var.zone
      subnet_id = yandex_vpc_subnet.custom-subnet.id
    }
    public_ip          = true
    maintenance_policy {
      auto_upgrade = false
    }
  }
  node_service_account_id = yandex_iam_service_account.node-sa.id
  service_account_id      = yandex_iam_service_account.admin-sa.id
}

provider "kubernetes" {
  host                   = yandex_kubernetes_cluster.allocator_cluster.master.0.external_v4_endpoint
  cluster_ca_certificate = yandex_kubernetes_cluster.allocator_cluster.master.0.cluster_ca_certificate
  token                  = data.yandex_client_config.client.iam_token
  experiments {
    manifest_resource = true
  }
  alias = "allocator"
}

resource "yandex_kubernetes_node_group" "workers-s" {
  cluster_id  = yandex_kubernetes_cluster.allocator_cluster.id
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

  provider = kubernetes.allocator
}