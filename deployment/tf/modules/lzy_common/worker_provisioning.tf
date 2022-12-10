resource "kubernetes_namespace" "fictive" {
  metadata {
    name = "fictive"
  }
}

resource "kubernetes_daemonset" "worker_cpu_fictive_containers" {
  metadata {
    name      = "worker-fictive-containers-cpu"
    namespace = kubernetes_namespace.fictive.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "worker-image-caching-cpu"
      }
    }
    template {
      metadata {
        labels = {
          name = "worker-image-caching-cpu"
        }
      }
      spec {
        container {
          image   = var.worker-image
          name    = "fictive-worker"
          command = ["tail", "-f", "/entrypoint.sh"]
        }
        node_selector = {
          type = "cpu"
        }
      }
    }
  }
}

resource "kubernetes_daemonset" "worker_gpu_fictive_containers" {
  metadata {
    name      = "worker-fictive-containers-gpu"
    namespace = kubernetes_namespace.fictive.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "worker-image-caching-gpu"
      }
    }
    template {
      metadata {
        labels = {
          name = "worker-image-caching-gpu"
        }
      }
      spec {
        container {
          image   = var.worker-image
          name    = "fictive-worker"
          command = ["tail", "-f", "/entrypoint.sh"]
        }
        node_selector = {
          type = "gpu"
        }
        toleration {
          key      = "sku"
          operator = "Equal"
          value    = "gpu"
          effect   = "NoSchedule"
        }
      }
    }
  }
}