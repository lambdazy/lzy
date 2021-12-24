terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
  }
}

data "yandex_client_config" "client" {}

data "yandex_vpc_subnet" "subnet" {
  subnet_id = var.subnet_id
}

resource "yandex_iam_service_account" "sa" {
  name        = "k8s-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}

resource "yandex_resourcemanager_folder_iam_binding" "admin" {
  folder_id = var.folder_id

  role = "admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "editor" {
  folder_id = data.yandex_vpc_subnet.subnet.folder_id

  role = "editor"

  members = [
    "serviceAccount:${yandex_iam_service_account.sa.id}",
  ]
}

resource "yandex_vpc_address" "lzy_kharon" {
  count = 0
  name  = "kharon"

  external_ipv4_address {
    zone_id = var.location
  }
}

resource "yandex_vpc_address" "lzy_backoffice" {
  count = 0
  name  = "backoffice"

  external_ipv4_address {
    zone_id = var.location
  }
}


resource "yandex_kubernetes_cluster" "main" {
  name        = var.installation_name
  description = "Main k8s cluster"

  network_id         = var.network_id
  cluster_ipv4_range = "10.20.0.0/16"
  cluster_ipv6_range = "fc00::/96"
  service_ipv6_range = "fc01::/112"
  master {
    zonal {
      zone      = var.location
      subnet_id = data.yandex_vpc_subnet.subnet.subnet_id
    }
    public_ip = true
    maintenance_policy {
      auto_upgrade = false
    }
  }
  node_service_account_id = yandex_iam_service_account.sa.id
  service_account_id      = yandex_iam_service_account.sa.id
}

resource "yandex_kubernetes_node_group" "lzy" {
  cluster_id = yandex_kubernetes_cluster.main.id
  name       = "lzypool"
  node_labels = {
    "type" = "lzy"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids = [data.yandex_vpc_subnet.subnet.subnet_id]
      ipv4       = true
      ipv6       = true
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
      size = var.lzy_count
    }
  }
}

resource "yandex_kubernetes_node_group" "cpu" {
  cluster_id = yandex_kubernetes_cluster.main.id
  name       = "cpupool"
  node_labels = {
    "type" = "cpu"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids = [data.yandex_vpc_subnet.subnet.subnet_id]
      ipv4       = true
      ipv6       = true
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
      size = var.cpu_count
    }
  }
}


resource "yandex_kubernetes_node_group" "gpu" {
  count      = 0
  cluster_id = yandex_kubernetes_cluster.main.id
  name       = "gpupool"
  node_labels = {
    "type" = "gpu"
  }

  instance_template {
    platform_id = "gpu-standard-v2"

    network_interface {
      subnet_ids = [data.yandex_vpc_subnet.subnet.subnet_id]
      ipv4       = true
      ipv6       = true
    }

    resources {
      gpus   = 1
      memory = 48
      cores  = 8
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
      size = var.gpu_count
    }
  }
}

module "lzy_common" {
  source = "../../lzy_common"

  kharon_public_ip           = ""
  backoffice_public_ip       = ""
  installation_name          = var.installation_name
  oauth-github-client-id     = var.oauth-github-client-id
  oauth-github-client-secret = var.oauth-github-client-secret
  cluster_id                 = yandex_kubernetes_cluster.main.id

  s3-access-key       = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  s3-secret-key       = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  s3-service-endpoint = "storage.yandexcloud.net"
  s3-bucket-name = "lzy-bucket-internal"

  backoffice-frontend-image = var.backoffice-frontend-image
}