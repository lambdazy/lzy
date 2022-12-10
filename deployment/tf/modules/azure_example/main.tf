terraform {
  backend "azurerm" {
    resource_group_name  = "lzy-example"
    storage_account_name = "lzystorage"
    container_name       = "terraformstate"
    key                  = "lzy.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

variable "ver" {
  default = "1.1"  # Place lzy docker images version here
  type = string
}

module "azure_common" {
  source                     = "git@github.com:lambdazy/lzy.git//deployment/tf/modules/azure_common"
  installation_name          = "lzy-example"  # Name of your installation
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "example"  # Postfix of your blob storage, must be unique for all azure storages
  ssl-enabled                = false  # To enable ssl, build your own backoffice with cert
  backoffice-backend-image   = "lzydock/lzy-backoffice-backend:master"
  backoffice-frontend-image  = "lzydock/lzy-backoffice-frontend:master"
  worker-image              = "lzydock/lzy-worker:master-" + var.ver
  kharon-image               = "lzydock/lzy-kharon:master-" + var.ver
  server-image               = "lzydock/lzy-server:master-" + var.ver
  whiteboard-image           = "lzydock/lzy-whiteboard:master-" + var.ver
}