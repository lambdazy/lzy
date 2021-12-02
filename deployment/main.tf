terraform {
  required_providers {
    azurerm    = {
      source  = "hashicorp/azurerm"
      version = "2.85.0"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
    helm       = {
      source = "hashicorp/helm"
    }
    random     = {
      source  = "hashicorp/random"
      version = "3.0.1"
    }
  }
  backend "azurerm" {
    resource_group_name  = "lzy-testing-terraformstate"
    storage_account_name = "lzytestingtfstatestorage"
    container_name       = "terraformstate"
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
  node_labels           = {
    type = "lzy"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "cpu" {
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.cpu_count
  node_labels           = {
    type = "cpu"
  }
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

resource "kubernetes_secret" "oauth_github" {
  metadata {
    name = "oauth-github"
  }

  data = {
    client-id     = var.oauth-github-client-id
    client-secret = var.oauth-github-client-secret
  }

  type = "Opaque"
}

resource "azurerm_public_ip" "lzy_backoffice" {
  domain_name_label   = "backoffice-${var.installation_name}"
  name                = "lzy-backoffice-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}
