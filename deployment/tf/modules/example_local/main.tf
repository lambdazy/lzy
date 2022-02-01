terraform {
  backend "local" {
  }
}

provider "azurerm" {
  features {}
}

module "azure_common" {
  source                     = "../azure_common"
  installation_name          = "lzy-example"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "testing"
}
