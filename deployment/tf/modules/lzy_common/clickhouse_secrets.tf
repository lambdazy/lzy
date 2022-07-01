resource "random_password" "clickhouse_user_password" {
  length  = 16
  special = false
  count   = 1
}

resource "kubernetes_secret" "clickhouse_secret" {
  for_each = toset(["default", kubernetes_namespace.server_namespace.metadata[0].name])
  metadata {
    name      = "clickhouse"
    namespace = each.key
  }
  data = {
    username = "clickhouse"
    password = random_password.clickhouse_user_password[0].result
  }

  type = "Opaque"
}