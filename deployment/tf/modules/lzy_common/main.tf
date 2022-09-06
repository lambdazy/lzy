terraform {
  required_providers {
    kubernetes = {
      version = ">=2.11.0"
    }
    helm = {
      version = ">=2.5.1"
    }
    random = {
      version = ">=3.0.1"
    }
    tls = {
      version = ">=3.4.0"
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
        node_selector = {
          type = "gpu"
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

resource "kubernetes_namespace" "server_namespace" {
  metadata {
    name = "server"
  }
}

resource "kubernetes_cluster_role" "pods_operations" {
  metadata {
    name = "pods-operations"
  }
  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "watch", "list", "create", "delete"]
  }
}

resource "kubernetes_role_binding" "server_pods_operations" {
  metadata {
    name = "server-pods-operations"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.pods_operations.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = "default"
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "network_policy_operations" {
  metadata {
    name = "network-policy-operations"
  }
  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["networkpolicies"]
    verbs      = ["get", "watch", "list", "create", "delete"]
  }
}

resource "kubernetes_role_binding" "server_network_policy_operations" {
  metadata {
    name = "server-network-policy-operations"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.network_policy_operations.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = "default"
    namespace = kubernetes_namespace.server_namespace.metadata[0].name
  }
}

resource "kubernetes_network_policy" "servant_to_kafka_traffic" {
  metadata {
    name = "servant-to-lzy-traffic"
  }
  spec {
    policy_types = ["Ingress", "Egress"]
    pod_selector {
      match_labels = {
        "lzy.ai/app" = "servant"
      }
    }
    egress {
      to {
        pod_selector {
          match_labels = {
            "app.kubernetes.io/component" = "kafka"
          }
        }
      }
      to {
        pod_selector {
          match_labels = {
            "lzy.ai/role" = "system"
          }
        }
      }
    }
    egress {
      ports {
        protocol = "UDP"
        port = "53"
      }
    }
    ingress {
      from {
        pod_selector {
          match_labels = {
            "app.kubernetes.io/component" = "kafka"
          }
        }
      }
      from {
        pod_selector {
          match_labels = {
            "lzy.ai/role" = "system"
          }
        }
      }
    }
  }
}
