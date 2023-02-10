resource "yandex_kubernetes_cluster" "main" {
  name        = var.installation_name
  description = "Main k8s cluster"
  release_channel = "RAPID"

  network_id              = var.network_id
  cluster_ipv4_range      = "10.20.0.0/16"
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

resource "yandex_kubernetes_node_group" "services" {
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "lzy-services"
  description = "Nodegroup for lzy services"
  node_labels = {
    "type" = "lzy"
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
      size = 7
    }
  }
}

provider "kubernetes" {
  host                   = yandex_kubernetes_cluster.main.master.0.external_v4_endpoint
  cluster_ca_certificate = yandex_kubernetes_cluster.main.master.0.cluster_ca_certificate
  token                  = data.yandex_client_config.client.iam_token
  experiments {
    manifest_resource = true
  }
}