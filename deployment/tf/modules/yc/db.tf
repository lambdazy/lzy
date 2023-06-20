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

  host {
    zone      = "ru-central1-a"
    subnet_id = yandex_vpc_subnet.custom-subnet.id
  }
}

resource "yandex_mdb_postgresql_database" "lzy_dbs" {
  for_each = module.k8s_deployment.db_data

  cluster_id = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.id
  name       = each.key
  owner      = each.value
  depends_on = [yandex_mdb_postgresql_user.lzy_users]
}

resource "yandex_mdb_postgresql_user" "lzy_users" {
  for_each = module.k8s_deployment.db_data

  cluster_id = yandex_mdb_postgresql_cluster.lzy_postgresql_cluster.id
  name       = each.key
  password   = each.value
  conn_limit = 30
  permission {
    database_name = each.key
  }
}