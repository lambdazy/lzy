terraform {
  backend "azurerm" {
    resource_group_name  = "lzy-testing-terraformstate"
    storage_account_name = "lzytestingtfstatestorage"
    container_name       = "terraformstate"
    key                  = "lzy-exp.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

module "azure_common" {
  source                     = "../azure_common"
  installation_name          = "lzy-exp"
  oauth-github-client-id     = "86be13043c9f96e17372"
  oauth-github-client-secret = "276612ba3cad5b137ac274ccc86740f6cd9a0e76"
  s3-postfics                = "exp"
  gpu_count                  = 1
  servant-image              = "lzydock/lzy-servant:exp"
  server-image               = "lzydock/lzy-server:exp"
  kharon-image               = "lzydock/lzy-kharon:exp"
}

