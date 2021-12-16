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
  s3-postfics = "prod"
}
