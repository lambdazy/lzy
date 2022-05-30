terraform {
  backend "azurerm" {
    resource_group_name  = "lzy-testing-terraformstate"
    storage_account_name = "lzytestingtfstatestorage"
    container_name       = "terraformstate"
    key                  = "lzy-exp.terraform.tfstate"
  }
}

module "azure_common" {
  source                     = "../azure_common"
  installation_name          = "lzy-exp"
  oauth-github-client-id     = ""
  oauth-github-client-secret = ""
  s3-postfics                = "exp"
  ssl-enabled                = false
  backoffice-backend-image   = "lzydock/lzy-backoffice-backend:master"
  backoffice-frontend-image  = "lzydock/lzy-backoffice-frontend:master"
  server-image = "lzydock/lzy-server:dev-1.9"
  servant-image = "lzydock/lzy-servant:dev-1.9"
  kharon-image = "lzydock/lzy-kharon:dev-1.9"
  whiteboard-image = "lzydock/lzy-whiteboard:dev-1.9"
}
