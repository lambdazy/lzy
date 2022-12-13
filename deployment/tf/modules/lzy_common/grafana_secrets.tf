resource "random_password" "grafana_password" {
  length  = 16
  special = false
  count   = 1
}

resource "kubernetes_secret" "grafana_secret" {
  metadata {
    name = "grafana"
  }
  data = {
    username = "admin"
    password = random_password.grafana_password[0].result
  }

  type = "Opaque"
}