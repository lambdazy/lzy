provider "yandex" {
  cloud_id                 = var.cloud_id
  service_account_key_file = "./lzy-sa"
  zone                     = var.location
  max_retries              = 10
}