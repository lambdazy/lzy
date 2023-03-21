resource "random_password" "kafka_password" {
  length   = 16
  special  = false
}

locals {
  kafka_admin_username = "kafka_admin"
}

resource "yandex_mdb_kafka_cluster" "main_kafka_cluster" {
  name                = "main_kafka"
  network_id          = var.network_id
  subnet_ids  = [yandex_vpc_subnet.custom-subnet.id]
  deletion_protection = false

  config {
    unmanaged_topics = true

    assign_public_ip = false
    brokers_count    = 1
    version          = "3.2"
    schema_registry  = "false"
    kafka {
      resources {
        disk_size          = 16
        disk_type_id       = "network-ssd"
        resource_preset_id = "s3-c2-m8"
      }
    }

    zones = [
      var.zone
    ]
  }
}

resource "kubernetes_secret" "kafka_secret" {
  metadata {
    name      = "kafka-secret"
  }

  data = {
    username = local.kafka_admin_username,
    password = random_password.kafka_password.result
  }

  type = "Opaque"
}