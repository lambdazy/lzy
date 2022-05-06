resource "kubernetes_namespace" "fictive" {
  metadata {
    name = "fictive"
  }
}

resource "kubernetes_daemonset" "servant_cpu_fictive_containers" {
  metadata {
    name      = "servant-fictive-containers-cpu"
    namespace = kubernetes_namespace.fictive.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "servant-image-caching-cpu"
      }
    }
    template {
      metadata {
        labels = {
          name = "servant-image-caching-cpu"
        }
      }
      spec {
        container {
          image = var.servant-image
          name  = "fictive-servant"
          command = ["tail", "-f", "/entrypoint.sh"]
        }
        node_selector = {
          type = "cpu"
        }
      }
    }
  }
}

resource "kubernetes_daemonset" "servant_gpu_fictive_containers" {
  metadata {
    name      = "servant-fictive-containers-gpu"
    namespace = kubernetes_namespace.fictive.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        name = "servant-image-caching-gpu"
      }
    }
    template {
      metadata {
        labels = {
          name = "servant-image-caching-gpu"
        }
      }
      spec {
        container {
          image = var.servant-image
          name  = "fictive-servant"
          command = ["tail", "-f", "/entrypoint.sh"]
        }
        node_selector = {
          type = "gpu"
        }
      }
    }
  }
}