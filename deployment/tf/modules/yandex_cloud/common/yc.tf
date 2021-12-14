terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
  }
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

resource "yandex_kubernetes_cluster" "main" {
  name = var.installation_name
  description = "Main k8s cluster"

  network_id = ""
  node_service_account_id = ""
  service_account_id = ""
  master {
    version = "1.17"
    zonal {
      zone = var.location
      subnet_id = ""
    }
    public_ip = false
    maintenance_policy {
      auto_upgrade = false
    }
  }
}

module "lzy_common" {
  source                            = "../../lzy_common"
  kubernetes_host                   = yandex_kubernetes_cluster.main.kube_config[0].host
  kubernetes_client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)
  kubernetes_client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_key)
  kubernetes_cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)
  kharon_public_ip                  = yandex_vpc_address.lzy_kharon.
  backoffice_public_ip              = azurerm_public_ip.lzy_backoffice.ip_address
  installation_name                 = var.installation_name
  oauth-github-client-id            = var.oauth-github-client-id
  oauth-github-client-secret        = var.oauth-github-client-secret
}