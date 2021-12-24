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
  gpu_count = 0
  server-image = "lzydock/lzy-server:testing-2"
  whiteboard-image = "lzydock/lzy-whiteboard:testing-2"
  kharon-image = "lzydock/lzy-kharon:testing-2"
  s3-postfics = "testing2"
  servant-image = "lzydock/lzy-servant:testing-2"
  oauth-github-client-id = var.github-client-id
  oauth-github-client-secret = var.github-secret
}
