provider "yandex" {
  cloud_id                 = var.cloud_id
  folder_id                = var.folder_id
  service_account_key_file = "./lzy-sa"
  zone                     = var.location
  max_retries              = 10
}