terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "2.85.0"
    }
  }
}

resource "azurerm_resource_group" "test" {
  name     = "${var.installation_name}-resources"
  location = var.location
}

resource "azurerm_virtual_network" "test" {
  name                = "${var.installation_name}-network"
  resource_group_name = azurerm_resource_group.test.name
  location            = azurerm_resource_group.test.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_kubernetes_cluster" "main" {
  name                = var.installation_name
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  dns_prefix          = var.installation_name

  default_node_pool {
    name        = "lzypool"
    vm_size     = "Standard_D2_v2"
    node_count  = 6
    node_labels = {
      type = "lzy"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    load_balancer_sku = "Standard"
    network_plugin    = "kubenet"
  }

  tags = {
    Environment = "Development"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "auto_scale_cpu" {
  count                 = var.cpu_pool_auto_scale && var.max_cpu_count > 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  min_count             = var.min_cpu_count
  max_count             = var.max_cpu_count
  enable_auto_scaling   = true
  node_labels           = {
    type = "cpu"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "fixed_cpu" {
  count                 = !var.cpu_pool_auto_scale && var.cpu_pool_size > 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.min_cpu_count
  enable_auto_scaling   = false
  node_labels           = {
    type = "cpu"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "gpu" {
  count                 = var.gpu_count != 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "gpupool"
  vm_size               = "Standard_NV12s_v3"
  node_count            = var.gpu_count
  enable_auto_scaling   = false
  availability_zones    = []
  node_labels           = {
    type = "gpu"
  }
  node_taints           = [
    "sku=gpu:NoSchedule"
  ]
}


resource "azurerm_public_ip" "lzy_kharon" {
  count               = var.create_public_kharon_service ? 1 : 0
  domain_name_label   = "kharon-${var.installation_name}"
  name                = "lzy-kharon-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}

resource "azurerm_public_ip" "grafana" {
  count               = var.create_public_grafana_service ? 1 : 0
  domain_name_label   = "grafana-${var.installation_name}"
  name                = "grafana-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}

resource "azurerm_role_assignment" "test" {
  scope                = azurerm_resource_group.test.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_kubernetes_cluster.main.identity[0].principal_id
}

resource "azurerm_public_ip" "lzy_backoffice" {
  count               = var.create_public_backoffice_service ? 1 : 0
  domain_name_label   = "backoffice-${var.installation_name}"
  name                = "lzy-backoffice-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}

module "lzy_common" {
  source                                         = "../lzy_common"
  kharon_public_ip                               = var.create_public_kharon_service ? azurerm_public_ip.lzy_kharon[0].ip_address : ""
  create_public_kharon_service                   = var.create_public_kharon_service
  backoffice_public_ip                           = var.create_public_backoffice_service ? azurerm_public_ip.lzy_backoffice[0].ip_address : ""
  create_public_backoffice_service               = var.create_public_backoffice_service
  grafana_public_ip                              = var.create_public_grafana_service ? azurerm_public_ip.grafana[0].ip_address : ""
  create_public_grafana_service                  = var.create_public_grafana_service
  kharon_load_balancer_necessary_annotations     = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  backoffice_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  grafana_load_balancer_necessary_annotations    = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  installation_name                              = var.installation_name
  oauth-github-client-id                         = var.oauth-github-client-id
  oauth-github-client-secret                     = var.oauth-github-client-secret
  s3-bucket-name                                 = "lzy-bucket"
  storage-provider                               = "azure"
  azure-connection-string                        = azurerm_storage_account.main_s3.primary_connection_string
  whiteboard-image                               = var.whiteboard-image
  server-image                                   = var.server-image
  kharon-image                                   = var.kharon-image
  backoffice-backend-image                       = var.backoffice-backend-image
  backoffice-frontend-image                      = var.backoffice-frontend-image
  clickhouse-image                               = var.clickhouse-image
  azure-resource-group                           = azurerm_resource_group.test.name
  ssl-enabled                                    = var.ssl-enabled
  ssl-cert                                       = var.ssl-cert
  ssl-cert-key                                   = var.ssl-cert-key
  ssl-keystore-password                          = var.ssl-keystore-password
  servant-image                                  = var.servant-image
  s3-separated-per-bucket                        = var.s3-separated-per-bucket
  base-env-default-image                         = var.base-env-default-image
}
