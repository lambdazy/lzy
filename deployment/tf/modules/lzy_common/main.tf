terraform {
  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
    helm = {
      source = "hashicorp/helm"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.0.1"
    }
  }
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
        labels = {
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

resource "kubernetes_cluster_role" "server_pods_operations" {
  metadata {
    name = "server-pods-operations"
  }
  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "watch", "list", "create", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "server_pods_operations" {
  metadata {
    name = "server-pods-operations"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "server-pods-operations"
  }
  subject {
    kind      = "ServiceAccount"
    name      = "default"
    namespace = "default"
  }
}
