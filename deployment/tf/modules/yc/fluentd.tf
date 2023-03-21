locals {
  fluentd-sa-key-json = {
    "id" : yandex_iam_service_account_key.node-sa-key.id
    "service_account_id" : yandex_iam_service_account_key.node-sa-key.service_account_id
    "created_at" : yandex_iam_service_account_key.node-sa-key.created_at
    "key_algorithm" : yandex_iam_service_account_key.node-sa-key.key_algorithm
    "public_key" : yandex_iam_service_account_key.node-sa-key.public_key
    "private_key" : yandex_iam_service_account_key.node-sa-key.private_key
  }
}

module "logs_main" {
  source = "./logs"

  node-sa-key = local.fluentd-sa-key-json
  folder_id   = var.folder_id

  providers = {
    kubernetes = kubernetes
  }
  fluent_port = local.fluent-bit-port
}

module "logs_allocator" {
  source = "./logs"

  node-sa-key = local.fluentd-sa-key-json
  folder_id   = var.folder_id

  providers = {
    kubernetes = kubernetes.allocator
  }
  fluent_port = local.fluent-bit-port
}
