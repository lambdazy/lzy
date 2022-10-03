locals {
  postgresql-configs = {
    "iam" = {
      db   = "iamDB",
      user = "iam"
    }
    "allocator" = {
      db   = "allocatorDb",
      user = "allocator"
    }
  }
}

resource "random_password" "postgresql_db_passwords" {
  for_each = local.postgresql-configs
  length   = 16
  special  = false
}

resource "yandex_mdb_postgresql_cluster" "lzy_postgresql_cluster" {
  name                = "lzy-postgresql-cluster"
  environment         = "PRODUCTION"
  network_id          = var.network_id
  folder_id           = var.folder_id
  description         = "postgresql cluster for Lzy databases"
  deletion_protection = false

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
    for_each = local.postgresql-configs
    content {
      name     = user.value.user
      password = random_password.postgresql_db_passwords[user.key].result
      permission {
        database_name = user.value.db
      }
    }
  }
  dynamic "database" {
    for_each = local.postgresql-configs
    content {
      name  = database.value.db
      owner = database.value.user
    }
  }

  host {
    zone      = var.zone
    subnet_id = yandex_vpc_subnet.postgres-net.id
  }
}