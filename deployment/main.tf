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
    key                  = "dev.terraform.tfstate"
  }
}

resource "azurerm_resource_group" "test" {
  name     = "lzy-testing-resources"
  location = var.location
}

resource "azurerm_virtual_network" "test" {
  name                = "lzy-testing-network"
  resource_group_name = azurerm_resource_group.test.name
  location            = azurerm_resource_group.test.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_kubernetes_cluster" "main" {
  name                = var.cluster_name
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  dns_prefix          = var.dns_prefix

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

resource "random_password" "postgres_password" {
  count   = 1
  length  = 16
  special = true
}

resource "kubernetes_secret" "postgres" {
  metadata {
    name = "postgres"
  }

  data = {
    postgresql-postgres-password = random_password.postgres_password[0].result
    postgresql-password          = random_password.postgres_password[0].result
    password                     = random_password.postgres_password[0].result
  }

  type = "Opaque"
}

resource "helm_release" "lzy_server_db" {
  name       = "postgres"
  chart      = "postgresql"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name  = "global.postgresql.postgresqlDatabase"
    value = "serverDB"
  }

  set {
    name  = "global.postgresql.postgresqlUsername"
    value = "server"
  }

  set {
    name  = "global.postgresql.existingSecret"
    value = kubernetes_secret.postgres.metadata[0].name
  }

  set_sensitive {
    name  = "global.postgresql.postgresqlPassword"
    value = random_password.postgres_password[0].result
  }

  set {
    name  = "global.postgresql.servicePort"
    value = "5432"
  }
}

resource "azurerm_public_ip" "lzy_kharon" {
  domain_name_label   = "kharon-lzy"
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

resource "kubernetes_secret" "backoffice_secrets" {
  metadata {
    name = "backoffice-secrets"
  }

  data = {
    private-key = var.backoffice-secrets-private-key
  }

  type = "Opaque"
}

resource "azurerm_public_ip" "lzy_backoffice" {
  domain_name_label   = "lzy-backoffice"
  name                = "lzy-backoffice-public-ip"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
}
