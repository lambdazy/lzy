terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
  }
}

data "yandex_client_config" "client" {}

resource "yandex_iam_service_account" "sa" {
  name        = "k8s-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_resourcemanager_folder_iam_binding" "admin" {
  folder_id = var.folder_id

  role = "editor"

  members = [
    "serviceAccount:${yandex_iam_service_account.sa.id}",
  ]
}

resource "yandex_vpc_address" "lzy_kharon" {
  name = "kharon"

  external_ipv4_address {
    zone_id = var.location
  }
}

resource "yandex_vpc_address" "lzy_backoffice" {
  name = "backoffice"

  external_ipv4_address {
    zone_id = var.location
  }
}

//resource "yandex_vpc_network" "main" {
//}

resource "yandex_kubernetes_cluster" "main" {
  name        = var.installation_name
  description = "Main k8s cluster"

  network_id              = var.network_id
  master {
    zonal {
      zone      = var.location
      subnet_id = var.subnet_id
    }
    public_ip = false
    maintenance_policy {
      auto_upgrade = false
    }
  }
  node_service_account_id = yandex_iam_service_account.sa.id
  service_account_id = yandex_iam_service_account.sa.id
}

//resource "yandex_kubernetes_node_group" "lzy" {
//  cluster_id = yandex_kubernetes_cluster.main.id
//  name       = "lzypool"
//  labels = {
//    "type" = "lzy"
//  }
//
//  instance_template {
//    platform_id = "standard-v2"
//  }
//
//  scale_policy {
//    fixed_scale {
//      size = var.lzy_count
//    }
//  }
//}

module "lzy_common" {
  source                            = "../../lzy_common"
  kharon_public_ip                  = yandex_vpc_address.lzy_kharon.external_ipv4_address.0.address
  backoffice_public_ip              = yandex_vpc_address.lzy_backoffice.external_ipv4_address.0.address
  installation_name                 = var.installation_name
  oauth-github-client-id            = var.oauth-github-client-id
  oauth-github-client-secret        = var.oauth-github-client-secret
}