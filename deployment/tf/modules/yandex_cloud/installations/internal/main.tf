terraform {
  backend "s3" {
    endpoint   = "storage.yandexcloud.net"
    bucket     = "lzy-internal-tf-state"
    region     = "us-east-1"
    key        = "lzy-internal-tf-state/lzy-internal.tfstate"
    access_key = var.s3-access-key
    secret_key = var.s3-secret-key

    skip_region_validation      = true
    skip_credentials_validation = true
  }
}

module "common" {
  source                     = "../../common"
  installation_name          = "lzy-yc-internal"
  cloud_id                   = "b1gfcpod5hbd1ivs7dav" //cloud-ai
  folder_id                  = "b1gsbp7ts9oepodda5a1" //lzy
  oauth-github-client-id     = var.oauth-github-client-id
  oauth-github-client-secret = var.oauth-github-secret
  network_id                 = "enps564p8bp649d38qgh" //cloud-ml-dev-nets
  subnet_id                  = "e9brt77u0kenestd9232" //cloud-ml-dev-nets-ru-central1-a
  YC_TOKEN                   = var.yc-token
  s3-separated-per-bucket    = true
  server-image = "lzydock/lzy-server:testing-2"
  servant-image = "lzydock/lzy-servant:testing-2"

  gpu_count = 0
}