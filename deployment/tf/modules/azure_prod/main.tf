terraform {
  backend "azurerm" {
    resource_group_name  = "lzy-testing-terraformstate"
    storage_account_name = "lzytestingtfstatestorage"
    container_name       = "terraformstate"
    key                  = "prod.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

module "azure_common" {
  source                     = "../azure_common"
  installation_name          = "lzy-prod"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "prod"
  ssl-enabled = var.ssl-enabled
  ssl-cert = file(var.ssl-cert-path)
  ssl-cert-key = file(var.ssl-cert-key-path)
  ssl-keystore-password = var.ssl-keystore-password
  backoffice-backend-image = "lzydock/lzy-backoffice-backend:master-ssl"
  backoffice-frontend-image = "lzydock/lzy-backoffice-frontend:master-ssl"
}
