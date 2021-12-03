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

resource "azurerm_kubernetes_cluster_node_pool" "gpu" {
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

resource "kubernetes_namespace" "gpu_resources" {
  metadata {
    name = "gpu-resources"
  }
}

resource "kubernetes_daemonset" "nvidia_plugin" {
  metadata {
    name      = "nvidia-device-plugin"
    namespace = kubernetes_namespace.gpu_resources.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name : "nvidia-device-plugin-ds"
      }
    }
    strategy {
      type = "RollingUpdate"
    }
    template {
      metadata {
        annotations = {
          "scheduler.alpha.kubernetes.io/critical-pod" = ""
        }
        labels      = {
          name = "nvidia-device-plugin-ds"
        }
      }
      spec {
        toleration {
          key      = "CriticalAddonsOnly"
          operator = "Exists"
        }
        toleration {
          key      = "nvidia.com/gpu"
          operator = "Exists"
          effect   = "NoSchedule"
        }
        toleration {
          key      = "sku"
          operator = "Equal"
          value    = "gpu"
          effect   = "NoSchedule"
        }
        container {
          image = "mcr.microsoft.com/oss/nvidia/k8s-device-plugin:1.11"
          name  = "nvidia-device-plugin"
          security_context {
            allow_privilege_escalation = false
            capabilities {
              drop = ["ALL"]
            }
          }
          volume_mount {
            mount_path = "/var/lib/kubelet/device-plugins"
            name       = "device-plugin"
          }
        }
        volume {
          name = "device-plugin"
          host_path {
            path = "/var/lib/kubelet/device-plugins"
          }
        }
      }
    }
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
