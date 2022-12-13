terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
    random = {
      version = ">=3.0.1"
    }
    kubernetes = {
      version = "=2.11.0"
    }
    helm = {
      version = "=2.6.0"
    }
  }
}

data "yandex_client_config" "client" {}

resource "yandex_resourcemanager_folder" "lzy_folder" {
  cloud_id    = var.cloud_id
  name        = "lzy"
  description = "Lzy service folder"
}

resource "yandex_vpc_network" "lzy_net" {
  folder_id   = yandex_resourcemanager_folder.lzy_folder.id
  name        = "lzy-net"
  description = "Lzy service network"
}

resource "yandex_vpc_subnet" "lzy-subnet" {
  v4_cidr_blocks = ["10.2.0.0/16"]
  zone           = var.location
  network_id     = yandex_vpc_network.lzy_net.id
  description    = "Lzy service subnet"
}

resource "yandex_iam_service_account" "sa" {
  name        = "${var.installation_name}-k8s-sa"
  description = "Service account to manage Lzy K8s"
}

resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "Static access key for object storage"
}

resource "yandex_resourcemanager_folder_iam_binding" "admin" {
  folder_id = yandex_resourcemanager_folder.lzy_folder.id

  role = "admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.sa.id}",
  ]
}

resource "yandex_kubernetes_cluster" "main" {
  name        = var.installation_name
  description = "Main k8s cluster"

  network_id              = yandex_vpc_network.lzy_net.id
  cluster_ipv4_range      = "10.20.0.0/16"
  master {
    zonal {
      zone      = var.location
      subnet_id = yandex_vpc_subnet.lzy-subnet.id
    }
    public_ip          = true
    maintenance_policy {
      auto_upgrade = false
    }
    security_group_ids = var.cluster-security-groups
  }
  release_channel         = "RAPID"
  network_policy_provider = "CILIUM"
  node_service_account_id = yandex_iam_service_account.sa.id
  service_account_id      = yandex_iam_service_account.sa.id
}

resource "yandex_iam_service_account_key" "server-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "key for server"
  key_algorithm      = "RSA_4096"
}

resource "yandex_kubernetes_node_group" "lzy" {
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "lzypool"
  node_labels = {
    "type" = "lzy"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids         = [yandex_vpc_subnet.lzy-subnet.id]
      ipv4               = true
      security_group_ids = var.nodes-security-groups
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
      #   server + db -> 2, kharon -> 1, whiteboard + db -> 2, clickhouse -> 1, grafana -> 2, kafka + zookeeper -> 2, backoffice -> 1, <reserve node> -> 1 = 12
      size = 12
    }
  }
}

resource "yandex_kubernetes_node_group" "fixed_cpu" {
  count       = var.cpu_pool_size > 0 ? 1 : 0
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "cpupool"
  node_labels = {
    "type" = "cpu"
  }

  instance_template {
    platform_id = "standard-v2"

    network_interface {
      subnet_ids         = [yandex_vpc_subnet.lzy-subnet.id]
      ipv4               = true
      security_group_ids = var.nodes-security-groups
    }

    resources {
      memory = 32
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
      size = var.cpu_pool_size
    }
  }
}

resource "yandex_kubernetes_node_group" "gpu" {
  count       = var.gpu_count != 0 ? 1 : 0
  cluster_id  = yandex_kubernetes_cluster.main.id
  name        = "gpupool"
  node_labels = {
    "type" = "gpu"
  }
  node_taints = [
    "sku=gpu:NoSchedule"
  ]

  instance_template {
    platform_id = "gpu-standard-v2"

    network_interface {
      subnet_ids         = [yandex_vpc_subnet.lzy-subnet.id]
      ipv4               = true
      security_group_ids = var.nodes-security-groups
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
#  source = "git@github.com:lambdazy/lzy.git//deployment/tf/modules/lzy_common?ref=dev"
  source = "../lzy_common"

  kharon_public_ip                 = var.kharon_public_ip
  create_public_kharon_service     = var.create_public_kharon_service
  backoffice_public_ip             = var.backoffice_public_ip
  create_public_backoffice_service = var.create_public_backoffice_service
  grafana_public_ip                = var.grafana_public_ip
  create_public_grafana_service    = var.create_public_grafana_service
  installation_name                = var.installation_name
  oauth-github-client-id           = var.oauth-github-client-id
  oauth-github-client-secret       = var.oauth-github-client-secret

  lzy_server_db_host     = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn
  lzy_server_db_port     = 6432
  lzy_server_db_name     = local.postgresql-configs.server.db
  lzy_server_db_user     = local.postgresql-configs.server.user
  lzy_server_db_password = random_password.postgresql_db_passwords["server"].result

  lzy_whiteboard_db_host     = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn
  lzy_whiteboard_db_port     = 6432
  lzy_whiteboard_db_name     = local.postgresql-configs.whiteboard.db
  lzy_whiteboard_db_user     = local.postgresql-configs.whiteboard.user
  lzy_whiteboard_db_password = random_password.postgresql_db_passwords["whiteboard"].result

  iam_db_host     = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn
  iam_db_port     = 6432
  iam_db_name     = local.postgresql-configs.iam.db
  iam_db_user     = local.postgresql-configs.iam.user
  iam_db_password = random_password.postgresql_db_passwords["iam"].result

  backoffice-backend-image   = var.backoffice-backend-image
  backoffice-frontend-image  = var.backoffice-frontend-image
  clickhouse-image           = var.clickhouse-image
  grafana-image              = var.grafana-image
  kharon-image               = var.kharon-image
  server-image               = var.server-image
  whiteboard-image           = var.whiteboard-image
  iam-image                  = var.iam-image
  worker-image              = var.worker-image
  default-env-image          = var.default-env-image

  amazon-access-key       = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  amazon-secret-key       = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  amazon-service-endpoint = "https://storage.yandexcloud.net"
  s3-bucket-name          = var.installation_name
  s3-separated-per-bucket = var.s3-separated-per-bucket

  ssl-enabled                = var.ssl-enabled

  server-additional-envs = {
    SERVER_YC_ENABLED            = "true",
    SERVER_YC_SERVICE_ACCOUNT_ID = yandex_iam_service_account.sa.id
    SERVER_YC_KEY_ID             = yandex_iam_service_account_key.server-key.id
    SERVER_YC_PRIVATE_KEY        = yandex_iam_service_account_key.server-key.private_key
    SERVER_YC_FOLDER_ID          = yandex_resourcemanager_folder.lzy_folder.id
  }
}
