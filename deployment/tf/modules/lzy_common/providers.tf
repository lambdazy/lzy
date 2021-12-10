provider "kubernetes" {
  host                   = var.kubernetes_host
  client_certificate     = var.kubernetes_client_certificate
  client_key             = var.kubernetes_client_key
  cluster_ca_certificate = var.kubernetes_client_certificate
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

provider "random" {
}
