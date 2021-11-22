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

resource "kubernetes_pod" "lzy_server" {
  metadata {
    name   = "lzy-server"
    labels = {
      app = "lzy-server"
    }
  }
  spec {
    container {
      name              = "lzy-server"
      image             = "celdwind/lzy:lzy-server"
      image_pull_policy = "Always"
      env {
        name = "LZY_SERVER_HOST"
        value_from {
          field_ref {
            field_path = "status.podIP"
          }
        }
      }
      env {
        name  = "AUTHENTICATOR"
        value = "DbAuthenticator"
      }
      env {
        name  = "DATABASE_URL"
        value = "jdbc:postgresql://postgres-postgresql.default.svc.cluster.local:5432/serverDB"
      }
      env {
        name  = "DATABASE_USERNAME"
        value = "server"
      }
      env {
        name = "DATABASE_PASSWORD"
        value_from {
          secret_key_ref {
            name = "postgres"
            key  = "postgresql-password"
          }
        }
      }
      port {
        container_port = 8888
      }
    }
    affinity {
      pod_anti_affinity {
        required_during_scheduling_ignored_during_execution {
          label_selector {
            match_expressions {
              key      = "app"
              operator = "In"
              values   = [
                "lzy-servant",
                "lzy-server",
                "lzy-kharon",
                "lzy-backoffice"
              ]
            }
          }
          topology_key = "kubernetes.io/hostname"
        }
      }
    }
    host_network = true
    dns_policy   = "ClusterFirstWithHostNet"
  }
}

resource "kubernetes_service" "lzy_server" {
  metadata {
    name        = "lzy-server-service"
    annotations = {
      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    port {
      port        = 8888
      target_port = 8888
    }
    selector = {
      app = "lzy-server"
    }
  }
}

resource "kubernetes_pod" "lzy_kharon" {
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
        value = kubernetes_service.lzy_server.spec[0].cluster_ip
      }
      port {
        container_port = 8899
      }
      port {
        container_port = 8900
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
    affinity {
      pod_anti_affinity {
        required_during_scheduling_ignored_during_execution {
          label_selector {
            match_expressions {
              key      = "app"
              operator = "In"
              values   = [
                "lzy-servant",
                "lzy-server",
                "lzy-kharon",
                "lzy-backoffice"
              ]
            }
          }
          topology_key = "kubernetes.io/hostname"
        }
      }
    }
    host_network = true
    dns_policy   = "ClusterFirstWithHostNet"
  }
}

resource "azurerm_public_ip" "lzy_kharon" {
  name                = "lzy_kharon_domain_name_label"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
  domain_name_label   = "lzy-kharon"
  reverse_fqdn        = "lzy.kharon.northeurope.cloudapp.azure.com"
}

resource "azurerm_role_assignment" "test" {
  scope                = azurerm_resource_group.test.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_kubernetes_cluster.main.identity[0].principal_id
}

resource "kubernetes_service" "lzy_kharon" {
  metadata {
    name        = "lzy-kharon-load-balancer"
    annotations = {
      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    load_balancer_ip = azurerm_public_ip.lzy_kharon.ip_address
    type             = "LoadBalancer"
    port {
      port = 8899
    }
    selector         = {
      app = "lzy-kharon"
    }
  }
}

resource "kubernetes_pod" "lzy_backoffice" {
  metadata {
    name   = "lzy-backoffice"
    labels = {
      app = "lzy-backoffice"
    }
  }
  spec {
    container {
      name              = "lzy-backoffice-frontend"
      image             = "celdwind/lzy:lzy-backoffice-frontend"
      image_pull_policy = "Always"
      port {
        container_port = 80
      }
    }
    container {
      name              = "lzy-backoffice-backend"
      image             = "celdwind/lzy:lzy-backoffice-backend"
      image_pull_policy = "Always"
      env {
        name  = "GRPC_PORT"
        value = "8888"
      }
      env {
        name = "OAUTH_GITHUB_CLIENT_ID"
        value_from {
          secret_key_ref {
            name = "oauth-github"
            key  = "client-id"
          }
        }
      }
      env {
        name  = "CREDENTIALS_USER_ID"
        value = "backoffice"
      }
      env {
        name = "OAUTH_GITHUB_CLIENT_SECRET"
        value_from {
          secret_key_ref {
            name = "oauth-github"
            key  = "client-secret"
          }
        }
      }
      env {
        name  = "CREDENTIALS_PRIVATE_KEY_PATH"
        value = "/etc/sec/backofficePrivateKey.txt"
      }
      volume_mount {
        mount_path = "sec"
        name       = "/etc/sec"
      }
      port {
        container_port = 8080
      }
    }
    volume {
      name = "sec"
      secret {
        secret_name = "backoffice-secrets"
        items {
          key  = "private-key"
          path = "backofficePrivateKey.txt"
        }
      }
    }
    affinity {
      pod_anti_affinity {
        required_during_scheduling_ignored_during_execution {
          label_selector {
            match_expressions {
              key      = "app"
              operator = "In"
              values   = [
                "lzy-servant",
                "lzy-server",
                "lzy-kharon",
                "lzy-backoffice"
              ]
            }
          }
          topology_key = "kubernetes.io/hostname"
        }
      }
    }
    host_network = true
    dns_policy   = "ClusterFirstWithHostNet"
  }
}

resource "azurerm_public_ip" "lzy_backoffice" {
  name                = "lzy_backoffice_domain_name_label"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
  sku                 = "Standard"
  allocation_method   = "Static"
  domain_name_label   = "lzy-backoffice"
  reverse_fqdn        = "lzy.backoffice.northeurope.cloudapp.azure.com"
}

resource "kubernetes_service" "lzy_backoffice" {
  metadata {
    name        = "lzy-backoffice-service"
    annotations = {
      "service.beta.kubernetes.io/azure-load-balancer-resource-group" = azurerm_resource_group.test.name
    }
  }
  spec {
    load_balancer_ip = azurerm_public_ip.lzy_backoffice.ip_address
    type             = "LoadBalancer"
    selector         = {
      app : "lzy-backoffice"
    }
    port {
      name = "backend"
      port = 8080
    }
    port {
      name = "frontend"
      port = 80
    }
  }
}
