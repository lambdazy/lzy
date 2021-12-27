provider "yandex" {
  cloud_id    = var.cloud_id
  folder_id   = var.folder_id
  zone        = var.location
  max_retries = 10
  token       = var.YC_TOKEN
}

provider "kubernetes" {
  host                   = yandex_kubernetes_cluster.main.master.0.external_v4_endpoint
  cluster_ca_certificate = yandex_kubernetes_cluster.main.master.0.cluster_ca_certificate
  token                  = data.yandex_client_config.client.iam_token
  experiments {
    manifest_resource = true
  }
}

provider "helm" {
  kubernetes {
    host                   = yandex_kubernetes_cluster.main.master.0.external_v4_endpoint
    cluster_ca_certificate = yandex_kubernetes_cluster.main.master.0.cluster_ca_certificate
    token                  = data.yandex_client_config.client.iam_token
  }
}