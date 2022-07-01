locals {
  grafana-labels = {
    app                         = "grafana"
    app.kubernetes.io / name    = "lzy-grafana"
    app.kubernetes.io / part-of = "lzy"
    lzy.ai / app                = "grafana"
  }
}

resource "kubernetes_deployment" "grafana" {
  metadata {
    name   = "grafana"
    labels = local.grafana-labels
  }
  spec {
    selector {
      match_labels = local.grafana-labels
    }
    template {
      metadata {
        name   = "grafana"
        labels = local.grafana-labels
      }
      spec {
        container {
          name  = "grafana"
          image = var.grafana-image
          env {
            name  = "GF_INSTALL_PLUGINS"
            value = "vertamedia-clickhouse-datasource"
          }
          env {
            name  = "CLICKHOUSE_URL"
            value = "http://${kubernetes_service.clickhouse_service.spec[0].cluster_ip}:8123"
          }
          env {
            name = "CLICKHOUSE_USER"
            value_from {
              secret_key_ref {
                name = "clickhouse"
                key  = "username"
              }
            }
          }
          env {
            name = "CLICKHOUSE_PASSWORD"
            value_from {
              secret_key_ref {
                name = "clickhouse"
                key  = "password"
              }
            }
          }
          env {
            name = "GF_SECURITY_ADMIN_PASSWORD"
            value_from {
              secret_key_ref {
                name = "grafana"
                key  = "password"
              }
            }
          }
          env {
            name  = "GF_AUTH_ANONYMOUS_ENABLED"
            value = "false"
          }
          port {
            container_port = 3000
            host_port      = 3000
          }
        }
        node_selector = {
          type = "lzy"
        }
        affinity {
          pod_anti_affinity {
            required_during_scheduling_ignored_during_execution {
              label_selector {
                match_expressions {
                  key      = "app"
                  operator = "In"
                  values   = local.all-services-k8s-app-labels
                }
                match_expressions {
                  key      = "app.kubernetes.io/managed-by"
                  operator = "In"
                  values   = ["Helm"]
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
  }
}

resource "kubernetes_service" "grafana_service" {
  count = var.create_public_grafana_service ? 1 : 0
  metadata {
    name        = "grafana-service"
    labels      = local.grafana-labels
    annotations = var.grafana_load_balancer_necessary_annotations
  }
  spec {
    load_balancer_ip = var.grafana_public_ip
    type             = "LoadBalancer"
    selector         = local.grafana-labels
    port {
      port = 3000
    }
  }
}