locals {
  lzy-node-k8s-label = "lzy"
  all-services-k8s-app-labels = [
    "lzy-worker",
    local.iam-labels.app,
    local.server-labels.app,
    local.kharon-labels.app,
    local.backoffice-labels.app,
    local.whiteboard-labels.app,
    local.grafana-labels.app,
    local.clickhouse-labels.app
  ]
}
