terraform {
  backend "azurerm" {
    resource_group_name  = "lzy-testing-terraformstate"
    storage_account_name = "lzytestingtfstatestorage"
    container_name       = "terraformstate"
    key                  = "test-2.terraform.tfsate"
  }
}

provider "azurerm" {
  features {}
}

module "azure_common" {
  source                     = "../azure_common"
  installation_name          = "lzy-testing-2"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "testing2"
  gpu_count                  = 0
  server-image = "lzydock/lzy-server:testing-2"
}
