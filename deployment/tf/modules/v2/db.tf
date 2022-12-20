locals {
  services = {
    iam = "iam"
    whiteboard = "whiteboard"
    allocator = "allocator"
    channel-manager = "channel-manager"
    lzy-service = "lzy-service"
    scheduler = "scheduler"
    graph-executor = "graph-executor"
    storage = "storage"
  }
}

resource "random_password" "db_passwords" {
  for_each = local.services
  length = 16
  special = false
}

resource "yandex_mdb_postgresql_cluster" "lzy_postgresql_cluster" {
  name                = "lzy-postgresql-cluster"
  environment         = "PRODUCTION"
  network_id          = data.yandex_vpc_network.custom-net.network_id
  folder_id           = var.folder_id
  description         = "postgresql cluster for Lzy databases"
  deletion_protection = true

  config {
    version = 13
    resources {
      resource_preset_id = "s2.micro"
      disk_type_id       = "network-ssd"
      disk_size          = 16
    }

    access {
      web_sql = true
    }

    postgresql_config = {
      max_connections                   = 300
      enable_parallel_hash              = true
      vacuum_cleanup_index_scale_factor = 0.2
      autovacuum_vacuum_scale_factor    = 0.34
      default_transaction_isolation     = "TRANSACTION_ISOLATION_READ_COMMITTED"
      shared_preload_libraries          = "SHARED_PRELOAD_LIBRARIES_AUTO_EXPLAIN,SHARED_PRELOAD_LIBRARIES_PG_HINT_PLAN"
    }
  }

  dynamic "user" {
    for_each = local.services
    content {
      conn_limit = 30
      name     = user.value
      password = random_password.db_passwords[user.key].result
      permission {
        database_name = user.value
      }
    }
  }
  dynamic "database" {
    for_each = local.services
    content {
      name  = database.value
      owner = database.value
    }
  }

  host {
    zone      = "ru-central1-a"
    subnet_id = yandex_vpc_subnet.custom-subnet.id
  }
}

resource "kubernetes_secret" "db_secret" {
  for_each = local.services
  metadata {
    name = "db-secret-${each.value}"
    namespace = "default"
  }

  data = {
    username = each.value,
    password = random_password.db_passwords[each.key].result,
    db_host = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.host[0].fqdn
    db_port = 6432
    db_name = each.value
  }

  type = "Opaque"
}