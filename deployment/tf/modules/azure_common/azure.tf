terraform {
  required_providers {
    azurerm = {
      version = ">=3.6.0"
    }
    kubernetes = {
      version = "=2.11.0"
    }
    helm = {
      version = "=2.6.0"
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
  name                              = var.installation_name
  location                          = azurerm_resource_group.test.location
  resource_group_name               = azurerm_resource_group.test.name
  dns_prefix                        = var.installation_name
  role_based_access_control_enabled = false

  default_node_pool {
    name       = "agentpool"
    node_count = 1
    vm_size    = "Standard_D2_v2"
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    load_balancer_sku = "standard"
    network_plugin    = "kubenet"
    network_policy    = "calico"
  }

  tags = {
    Environment = "Development"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "lzy" {
  count                 = 1
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "lzypool"
  vm_size               = "Standard_D2_v2"
  #   server + db -> 2, kharon -> 1, whiteboard + db -> 2, clickhouse -> 1, grafana -> 2, kafka + zookeeper -> 2, backoffice -> 1, <reserve node> -> 1 = 12
  node_count = 12
  node_labels = {
    type = "lzy"
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
  node_labels = {
    type = "cpu"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "fixed_cpu" {
  count                 = !var.cpu_pool_auto_scale && var.cpu_pool_size > 0 ? 1 : 0
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  name                  = "cpupool"
  vm_size               = "Standard_D2_v2"
  node_count            = var.cpu_pool_size
  enable_auto_scaling   = false
  node_labels = {
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
  node_labels = {
    type = "gpu"
  }
  node_taints = [
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
  source                           = "../lzy_common"
  kharon_public_ip                 = var.create_public_kharon_service ? azurerm_public_ip.lzy_kharon[0].ip_address : ""
  create_public_kharon_service     = var.create_public_kharon_service
  backoffice_public_ip             = var.create_public_backoffice_service ? azurerm_public_ip.lzy_backoffice[0].ip_address : ""
  create_public_backoffice_service = var.create_public_backoffice_service
  grafana_public_ip                = var.create_public_grafana_service ? azurerm_public_ip.grafana[0].ip_address : ""
  create_public_grafana_service    = var.create_public_grafana_service
  kharon_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  backoffice_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  grafana_load_balancer_necessary_annotations = {
    "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
  }
  installation_name          = var.installation_name
  oauth-github-client-id     = var.oauth-github-client-id
  oauth-github-client-secret = var.oauth-github-client-secret

  lzy_server_db_host     = "postgres-postgresql.server.svc.cluster.local"
  lzy_server_db_port     = 5432
  lzy_server_db_name     = "serverDB"
  lzy_server_db_user     = "server"
  lzy_server_db_password = ""

  lzy_whiteboard_db_host     = "whiteboard-postgresql.default.svc.cluster.local"
  lzy_whiteboard_db_port     = 5432
  lzy_whiteboard_db_name     = "whiteboards"
  lzy_whiteboard_db_user     = "whiteboard"
  lzy_whiteboard_db_password = ""

  iam_db_host     = "iam-postgresql.default.svc.cluster.local"
  iam_db_port     = 5432
  iam_db_name     = "iamDB"
  iam_db_user     = "iam"
  iam_db_password = ""

  s3-bucket-name            = "lzy-bucket"
  storage-provider          = "azure"
  azure-connection-string   = azurerm_storage_account.main_s3.primary_connection_string
  azure-resource-group      = azurerm_resource_group.test.name
  ssl-enabled               = var.ssl-enabled
  ssl-cert                  = var.ssl-cert
  ssl-cert-key              = var.ssl-cert-key
  ssl-keystore-password     = var.ssl-keystore-password
  s3-separated-per-bucket   = var.s3-separated-per-bucket

  backoffice-backend-image   = var.backoffice-backend-image
  backoffice-frontend-image  = var.backoffice-frontend-image
  clickhouse-image           = var.clickhouse-image
  grafana-image              = var.grafana-image
  kharon-image               = var.kharon-image
  server-image               = var.server-image
  whiteboard-image           = var.whiteboard-image
  iam-image                  = var.iam-image
  servant-image              = var.servant-image
  default-env-image          = var.default-env-image
}
