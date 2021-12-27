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
  count = var.lzy_count != 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "lzypool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.lzy_count
  node_labels = {
    type = "lzy"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "cpu" {
  count = var.cpu_count != 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.cpu_count
  node_labels = {
    type = "cpu"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "gpu" {
  count = var.gpu_count != 0 ? 1 : 0
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
  source               = "../lzy_common"
  kharon_public_ip     = azurerm_public_ip.lzy_kharon.ip_address
  backoffice_public_ip = azurerm_public_ip.lzy_backoffice.ip_address
  kharon_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  backoffice_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  installation_name                 = var.installation_name
  oauth-github-client-id            = var.oauth-github-client-id
  oauth-github-client-secret        = var.oauth-github-client-secret
  cluster_id                        = azurerm_kubernetes_cluster.main.id
  kubernetes_host                   = azurerm_kubernetes_cluster.main.kube_config.0.host
  kubernetes_client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)
  kubernetes_client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_key)
  kubernetes_cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)
  s3-bucket-name                    = "lzy-bucket"
  storage-provider                  = "azure"
  azure-connection-string           = azurerm_storage_account.main_s3.primary_connection_string
  whiteboard-image                  = var.whiteboard-image
  server-image                      = var.server-image
  kharon-image                      = var.kharon-image
  backoffice-backend-image          = var.backoffice-backend-image
  backoffice-frontend-image         = var.backoffice-frontend-image
  clickhouse-image                  = var.clickhouse-image
  azure-resource-group              = azurerm_resource_group.test.name
  ssl-enabled                       = var.ssl-enabled
  ssl-cert                          = var.ssl-cert
  ssl-cert-key                      = var.ssl-cert-key
  ssl-keystore-password             = var.ssl-keystore-password
  servant-image                     = var.servant-image
}
