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
    name       = "agentpool"
    node_count = var.agent_count
    vm_size    = "Standard_D2_v2"
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

resource "azurerm_kubernetes_cluster_node_pool" "lzy" {
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "lzypool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.lzy_count
  node_labels = {
    type = "lzy"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "cpu" {
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.cpu_count
  node_labels = {
    type = "cpu"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "gpu" {
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "gpupool"
  vm_size               = "Standard_NV12s_v3"
  node_count            = var.gpu_count
  enable_auto_scaling   = false
  availability_zones    = []
  node_labels = {
    type = "gpu"
  }
  node_taints = [
    "sku=gpu:NoSchedule"
  ]
}


resource "azurerm_public_ip" "lzy_kharon" {
  domain_name_label   = "kharon-${var.installation_name}"
  name                = "lzy-kharon-public-ip"
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
  domain_name_label   = "backoffice-${var.installation_name}"
  name                = "lzy-backoffice-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}

module "lzy_common" {
  source                            = "../lzy_common"
  kharon_public_ip                  = azurerm_public_ip.lzy_kharon.ip_address
  backoffice_public_ip              = azurerm_public_ip.lzy_backoffice.ip_address
  installation_name                 = var.installation_name
  oauth-github-client-id            = var.oauth-github-client-id
  oauth-github-client-secret        = var.oauth-github-client-secret
  cluster_id = azurerm_kubernetes_cluster.main.id
}
