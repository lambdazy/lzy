provider "yandex" {
  cloud_id                 = var.cloud_id
  folder_id                = var.folder_id
  service_account_key_file = "./lzy-sa"
  zone                     = var.location
  max_retries              = 10
}

provider "kubernetes" {
  host                   = yandex_kubernetes_cluster.main.master.0.external_v4_endpoint
  cluster_ca_certificate = yandex_kubernetes_cluster.main.master.0.cluster_ca_certificate
  token                  = data.yandex_client_config.client.iam_token
  config_path = "~/.kube/config"
}