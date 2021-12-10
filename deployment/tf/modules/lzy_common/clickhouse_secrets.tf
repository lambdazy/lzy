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