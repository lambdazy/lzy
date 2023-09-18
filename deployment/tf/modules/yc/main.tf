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
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.82.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = ">= 1.7.0"
    }
  }
}

data "yandex_client_config" "client" {}

module "k8s_deployment" {
  source = "../k8s"

  allocator-image           = var.allocator-image
  backoffice-backend-image  = var.backoffice-backend-image
  backoffice-frontend-image = var.backoffice-frontend-image
  channel-manager-image     = var.channel-manager-image
  s3-sink-image             = var.s3-sink-image
  scheduler-image           = var.scheduler-image
  unified-agent-image       = var.unified-agent-image
  lzy-service-image         = var.lzy-service-image
  servant-image             = var.servant-image
  storage-image             = var.storage-image
  whiteboard-image          = var.whiteboard-image
  iam-image                 = var.iam-image
  graph-image               = var.graph-image

  installation_name             = var.installation_name
  yc-endpoint                   = var.yc-endpoint
  folder_id                     = var.folder_id
  custom-subnet-id              = yandex_vpc_subnet.custom-subnet.id
  backoffice_public_ip          = yandex_vpc_address.backoffice_public_ip.external_ipv4_address[0].address
  whiteboard_public_ip          = yandex_vpc_address.whiteboard_public_ip.external_ipv4_address[0].address
  workflow_public_ip            = yandex_vpc_address.workflow_public_ip.external_ipv4_address[0].address
  db-host                       = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn
  oauth-github-client-id        = var.oauth-github-client-id
  oauth-github-client-secret    = var.oauth-github-client-secret
  node-sa-static-key-access-key = yandex_iam_service_account_static_access_key.admin-sa-static-key.access_key
  node-sa-static-key-secret-key = yandex_iam_service_account_static_access_key.admin-sa-static-key.secret_key
  pool-cluster-id               = yandex_kubernetes_cluster.allocator_cluster.id
  service-cluster-id            = yandex_kubernetes_cluster.main.id
  allocator_service_cidrs       = yandex_vpc_subnet.custom-subnet.v4_cidr_blocks

  providers = {
    kubernetes = kubernetes
  }
}