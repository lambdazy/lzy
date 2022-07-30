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
  backoffice-backend-image   = var.backoffice-backend-image
  backoffice-frontend-image  = var.backoffice-frontend-image
  clickhouse-image           = var.clickhouse-image
  grafana-image              = var.grafana-image
  kharon-image               = var.kharon-image
  server-image               = var.server-image
  whiteboard-image           = var.whiteboard-image
  iam-image                  = var.iam-image
  servant-image              = var.servant-image
  default-env-image          = var.default-env-image
}
