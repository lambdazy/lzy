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

resource "yandex_iam_service_account_key" "server-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "key for server"
  key_algorithm      = "RSA_4096"
}

resource "yandex_kubernetes_node_group" "lzy" {
  count = var.lzy_count != 0 ? 1 : 0
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
  count = var.cpu_count != 0 ? 1 : 0
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
  count = var.gpu_count != 0 ? 1 : 0
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

  amazon-access-key       = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  amazon-secret-key       = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  amazon-service-endpoint = "https://storage.yandexcloud.net"
  s3-bucket-name          = "lzy-bucket-internal"

  servant-image             = var.servant-image
  s3-separated-per-bucket   = var.s3-separated-per-bucket
  server-image              = var.server-image
  server-additional-envs = {
    SERVER_YC_ENABLED = "true",
    SERVER_YC_SERVICE_ACCOUNT_ID = yandex_iam_service_account.sa.id
    SERVER_YC_KEY_ID = yandex_iam_service_account_key.server-key.id
    SERVER_YC_PRIVATE_KEY = yandex_iam_service_account_key.server-key.private_key
    SERVER_YC_FOLDER_ID = var.folder_id
  }
}