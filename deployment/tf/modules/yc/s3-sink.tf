locals {
  s3-sink-labels = {
    app                         = "s3-sink"
    "app.kubernetes.io/name"    = "s3-sink"
    "lzy.ai/app"                = "s3-sink"
  }
  s3-sink-k8s-name = "s3-sink"
  s3-sink-image    = var.s3-sink-image
  s3-sink-port     = 23920
}

resource "kubernetes_deployment" "s3_sink" {
  count = var.s3_sink_enabled ? 1 : 0

  metadata {
    name   = local.s3-sink-k8s-name
    labels = local.s3-sink-labels
  }
  spec {
    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = local.s3-sink-labels
    }

    template {
      metadata {
        name   = local.s3-sink-k8s-name
        labels = local.s3-sink-labels
      }

      spec {
        container {
          name              = local.s3-sink-k8s-name
          image             = local.s3-sink-image
          image_pull_policy = "Always"

          port {
            container_port = local.s3-sink-port
          }

          dynamic "env" {
            for_each = local.kafka_env_map

            content {
              name  = "S3_SINK_${env.key}"
              value = env.value
            }
          }

          env {
            name = "S3_SINK_ADDRESS"
            value = "0.0.0.0:${local.s3-sink-port}"
          }

          env {
            name  = "S3_SINK_KAFKA_BOOTSTRAP_SERVERS"
            value = module.kafka[0].internal-bootstrap
          }

          volume_mount {
            mount_path = "/truststore"
            name       = "truststore"
          }

          volume_mount {
            mount_path = "/keystore"
            name       = "keystore"
          }
        }

        volume {
          name = "truststore"
          secret {
            secret_name = module.kafka[0].truststore-secret-name
            items {
              key  = "ca.p12"
              path = "truststore.p12"
            }
          }
        }

        volume {
          name = "keystore"
          secret {
            secret_name = module.kafka[0].keystore-secret-name
            items {
              key  = "cluster-operator.p12"
              path = "keystore.p12"
            }
          }
        }

        node_selector = {
          type = "lzy"
        }
      }
    }
  }
}

resource "kubernetes_service" "s3_sink_service" {
  count = var.s3_sink_enabled ? 1 : 0
  metadata {
    name        = "${local.s3-sink-k8s-name}-load-balancer"
    labels      = local.s3-sink-labels
  }
  spec {
    selector         = local.s3-sink-labels
    port {
      port        = local.s3-sink-port
      target_port = local.s3-sink-port
    }
    type = "ClusterIP"
  }
}