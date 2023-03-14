resource "yandex_iam_service_account" "admin-sa" {
  name        = "${var.installation_name}-k8s-lzy-admin-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_iam_service_account" "node-sa" {
  name        = "${var.installation_name}-k8s-lzy-node-sa"
  description = "service account for kuber nodes"
}

resource "yandex_resourcemanager_folder_iam_binding" "puller" {
  folder_id = var.folder_id

  role = "container-registry.images.puller"

  members = [
    "serviceAccount:${yandex_iam_service_account.node-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "monitoring-editor" {
  folder_id = var.folder_id

  role = "monitoring.editor"

  members = [
    "serviceAccount:${yandex_iam_service_account.node-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "logging-writer" {
  folder_id = var.folder_id

  role = "logging.writer"

  members = [
    "serviceAccount:${yandex_iam_service_account.node-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "admin" {
  folder_id = var.folder_id

  role = "editor"

  members = [
    "serviceAccount:${yandex_iam_service_account.admin-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "s3-admin" {
  folder_id = var.folder_id

  role = "storage.admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.admin-sa.id}",
  ]
}

resource "yandex_iam_service_account" "allocator-sa" {
  name        = "${var.installation_name}-k8s-sa"
  description = "service account to manage Lzy K8s"
}

resource "yandex_resourcemanager_folder_iam_binding" "allocator-admin" {
  folder_id = var.folder_id

  role = "k8s.admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.allocator-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "allocator-cluster-admin" {
  folder_id = var.folder_id

  role = "k8s.cluster-api.cluster-admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.allocator-sa.id}",
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "allocator-compute-admin" {
  folder_id = var.folder_id

  role = "compute.admin"

  members = [
    "serviceAccount:${yandex_iam_service_account.allocator-sa.id}",
  ]
}

resource "yandex_iam_service_account_key" "allocator-sa-key" {
  service_account_id = yandex_iam_service_account.allocator-sa.id
  description        = "key for allocator"
  key_algorithm      = "RSA_4096"
}

resource "yandex_iam_service_account_key" "node-sa-key" {
  service_account_id = yandex_iam_service_account.node-sa.id
  description        = "key for node"
  key_algorithm      = "RSA_4096"
}

resource "yandex_iam_service_account_key" "admin-sa-key" {
  service_account_id = yandex_iam_service_account.admin-sa.id
  description        = "key for storage"
  key_algorithm      = "RSA_4096"
}

resource "yandex_iam_service_account_static_access_key" "admin-sa-static-key" {
  service_account_id = yandex_iam_service_account.admin-sa.id
  description        = "static key for storage"
}