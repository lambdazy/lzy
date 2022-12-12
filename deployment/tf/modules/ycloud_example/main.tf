terraform {
  backend "s3" {
    endpoint   = "storage.yandexcloud.net"
    bucket     = "lzy-tf-state"
    region     = "us-east-1"
    key        = "lzy-tf-state/lzy.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
}


module "azure_common" {
#  source                     = "git@github.com:lambdazy/lzy.git//deployment/tf/modules/ycloud_common"
  source                     = "../ycloud_common"
  installation_name          = "lzy-example"  # Name of your installation
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "example"  # Postfix of your blob storage, must be unique for all azure storages
  ssl-enabled                = false  # To enable ssl, build your own backoffice with cert

  worker-image              = "lzydock/lzy-worker:<enter version here>"
  kharon-image               = "lzydock/lzy-kharon:<enter version here>"
  server-image               = "lzydock/lzy-server:<enter version here>"
  whiteboard-image           = "lzydock/lzy-whiteboard:<enter version here>"
  default-env-image          = "lzydock/default-env:<enter version here>"
  grafana-image              = "lzydock/lzy-grafana:<enter version here>"
  iam-image                  = "lzydock/iam:<enter version here>"

  cloud_id                   = ""
}