locals {
  fluentd-service-sa-key-json = {
    "id" : yandex_iam_service_account_key.service-node-sa-key.id
    "service_account_id" : yandex_iam_service_account_key.service-node-sa-key.service_account_id
    "created_at" : yandex_iam_service_account_key.service-node-sa-key.created_at
    "key_algorithm" : yandex_iam_service_account_key.service-node-sa-key.key_algorithm
    "public_key" : yandex_iam_service_account_key.service-node-sa-key.public_key
    "private_key" : yandex_iam_service_account_key.service-node-sa-key.private_key
  }
  fluentd-pool-sa-key-json = {
    "id" : yandex_iam_service_account_key.pool-node-sa-key.id
    "service_account_id" : yandex_iam_service_account_key.pool-node-sa-key.service_account_id
    "created_at" : yandex_iam_service_account_key.pool-node-sa-key.created_at
    "key_algorithm" : yandex_iam_service_account_key.pool-node-sa-key.key_algorithm
    "public_key" : yandex_iam_service_account_key.pool-node-sa-key.public_key
    "private_key" : yandex_iam_service_account_key.pool-node-sa-key.private_key
  }
}

module "logs_main" {
  source = "./logs"

  node-sa-key = local.fluentd-service-sa-key-json
  folder_id   = var.folder_id

  providers = {
    kubernetes = kubernetes
  }
}

module "logs_allocator" {
  source = "./logs"

  node-sa-key = local.fluentd-pool-sa-key-json
  folder_id   = var.folder_id

  providers = {
    kubernetes = kubernetes.allocator
  }
}
