resource "azurerm_storage_account" "main_s3" {
  name                   = "mains3${var.s3-postfics}"
  account_replication_type = "LRS"
  account_tier = "Standard"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

