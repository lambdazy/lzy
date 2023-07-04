locals {
  metrics_namespaces = {
    allocator       = {
      name = "allocator"
      port = local.allocator-metrics-port
    }
    channel-manager = {
      name = "channel-manager"
      port = local.channel-manager-metrics-port
    }
    graph-executor  = {
      name = "graph-executor"
      port = local.graph-executor-metrics-port
    }
    iam             = {
      name = "iam"
      port = local.iam-metrics-port
    }
    lzy-service     = {
      name = "lzy-service"
      port = local.lzy-service-metrics-port
    }
    scheduler       = {
      name = "scheduler"
      port = local.scheduler-metrics-port
    }
    site            = {
      name = "site"
      port = local.site-metrics-port
    }
    storage         = {
      name = "storage"
      port = local.storage-metrics-port
    }
    whiteboard      = {
      name = "whiteboard"
      port = local.whiteboard-metrics-port
    }
  }
}

resource "kubernetes_config_map" "unified-agent-config" {
  for_each = local.metrics_namespaces
  metadata {
    name = "unified-agent-config-${each.value.name}"
  }
  binary_data = {
    "config" = base64encode(templatefile("${path.module}/configs/unified-agent-config.yml", {
      namespace = each.value.name
      port      = each.value.port
    }))
  }
}

resource "kubernetes_config_map" "frontend-nginx-ssl-config" {
  count = var.ssl-enabled ? 1 : 0
  metadata {
    name = "frontend-nginx-ssl-config"
  }
  data = {
    "config" = templatefile("${path.module}/configs/nginx-ssl.conf", {
      domain-name = var.domain_name
    })
  }
}