terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "2.85.0"
    }
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

  #  linux_profile {
  #    admin_username = "ubuntu"
  #
  #    ssh_key {
  #      key_data = file(var.ssh_public_key)
  #    }
  #  }

  default_node_pool {
    name       = "agentpool"
    node_count = var.agent_count
    vm_size    = "Standard_D2_v2"
  }

  #  service_principal {
  #    client_id     = var.client_id
  #    client_secret = var.client_secret
  #  }
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

resource "kubernetes_pod" "lzy-server" {
  metadata {
    name = "lzy-server"
    labels = {
      app = "lzy-server"
    }
  }
  spec {
    container {
      image             = "celdwind/lzy:lzy-server"
      image_pull_policy = "Always"
      name              = "lzy-server"
      env {
        name = "LZY_SERVER_HOST"
        value_from {
          field_ref {
            field_path = "status.podIP"
          }
        }
      }
    }
    host_network = true
    dns_policy   = "Default"
  }
}

resource "kubernetes_service" "lzy-server-service" {
  metadata {
    name = "lzy-server-service"
  }
  spec {
    port {
      port = 8888
    }
    selector = {
      app = "lzy-server"
    }
  }
}

resource "kubernetes_pod" "lzy-kharon" {
  metadata {
    name   = "lzy-kharon"
    labels = {
      app : "lzy-kharon"
    }
  }
  spec {
    container {
      name              = "lzy-kharon"
      image             = "celdwind/lzy:lzy-kharon"
      image_pull_policy = "Always"
      env {
        name = "LZY_HOST"
        value_from {
          field_ref {
            field_path = "status.podIP"
          }
        }
      }
      env {
        name  = "LZY_SERVER_IP"
        value = kubernetes_service.lzy-server-service.spec[0].cluster_ip
      }
      args              = [
        "--lzy-server-address",
        "http://$(LZY_SERVER_IP):8888",
        "--host",
        "$(LZY_HOST)",
        "--port",
        "8899",
        "--servant-proxy-port",
        "8900"
      ]
    }
    host_network = true
    dns_policy   = "Default"
  }
}

resource "azurerm_public_ip" "test" {
  name                = "lzy_kharon_domain_name_label"
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

resource "kubernetes_service" "lzy-kharon-service" {
  metadata {
    annotations = {
      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
    name        = "lzy-kharon-service"
  }
  spec {
    load_balancer_ip = azurerm_public_ip.test.ip_address
    type             = "LoadBalancer"
    port {
      port = 8899
    }
    selector         = {
      app = "lzy-kharon"
    }
  }
}
