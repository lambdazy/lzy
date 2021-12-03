resource "random_password" "clickhouse_user_password" {
  length = 16
  special = false
  count = 1
}

resource "kubernetes_secret" "clickhouse_secret" {
  metadata {
    name = "clickhouse"
  }
  data = {
    username = "clickhouse"
    password = random_password.clickhouse_user_password[0].result
  }

  type = "Opaque"
}

resource "kubernetes_pod" "clickhouse" {
  metadata {
    name   = "clickhouse"
    labels = {
      app = "clickhouse"
    }
  }
  spec {
    container {
      name              = "clickhouse"
      image             = "clickhouse/clickhouse-server"
      env {
        name = "CLICKHOUSE_USER"
        value_from {
          secret_key_ref {
            name = "clickhouse"
            key = "username"
          }
        }
      }
      env {
        name = "CLICKHOUSE_PASSWORD"
        value_from {
          secret_key_ref {
            name = "clickhouse"
            key = "password"
          }
        }
      }
      env {
        name = "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"
        value = "1"
      }
      port {
        container_port = 9000
      }
    }
  }
}