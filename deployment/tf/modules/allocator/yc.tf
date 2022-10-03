provider "yandex" {
  endpoint = var.yc-endpoint
  cloud_id    = var.cloud_id
  folder_id   = var.folder_id
  zone        = var.zone
  max_retries = 2
  token = var.yc-token
}

data "yandex_client_config" "client" {}

resource "yandex_vpc_subnet" "postgres-net" {
  name = "postgres-net"
  network_id = var.network_id
  folder_id = var.folder_id
  zone = var.zone
  v4_cidr_blocks = ["192.168.10.0/24"]
}

resource "yandex_iam_service_account" "admin-sa" {
  name        = "k8s-lzy-admin-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_resourcemanager_folder_iam_binding" "admin" {
  folder_id = var.folder_id

  role = "admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.admin-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "net-admin" {
  folder_id = var.subnet-folder

  role = "admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.admin-sa.id}",
  ]
}

resource "yandex_iam_service_account" "allocator-sa" {
  name        = "k8s-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_resourcemanager_folder_iam_binding" "allocator-admin" {
  folder_id = var.folder_id

  role = "k8s.admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.allocator-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "allocator-cluster-admin" {
  folder_id = var.folder_id

  role = "k8s.cluster-api.cluster-admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.allocator-sa.id}",
  ]
}

resource "yandex_kubernetes_cluster" "main" {
  name        = var.installation_name
  description = "Main k8s cluster"
  release_channel = "RAPID"

  network_id              = var.network_id
  cluster_ipv4_range      = "10.20.0.0/16"
  master {
    zonal {
      zone      = var.zone
      subnet_id = var.subnet-id
    }
    public_ip          = true
    maintenance_policy {
      auto_upgrade = false
    }
  }
  node_service_account_id = yandex_iam_service_account.admin-sa.id
  service_account_id      = yandex_iam_service_account.admin-sa.id
}

resource "yandex_iam_service_account_key" "allocator-sa-key" {
  service_account_id = yandex_iam_service_account.allocator-sa.id
  description        = "key for allocator"
  key_algorithm      = "RSA_4096"
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
      subnet_ids         = [var.subnet-id]
      ipv4               = true
      ipv6               = true
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
      size = 2
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

