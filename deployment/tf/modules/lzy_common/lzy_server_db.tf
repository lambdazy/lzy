resource "random_password" "lzy_server_db_password" {
  count   = 1
  length  = 16
  special = false
}

resource "kubernetes_secret" "lzy_server_db" {
  metadata {
    name = "postgres"
  }

  data = {
    postgresql-postgres-password = random_password.lzy_server_db_password[0].result
    postgresql-password          = random_password.lzy_server_db_password[0].result
    postgres-password            = random_password.lzy_server_db_password[0].result
    password                     = random_password.lzy_server_db_password[0].result
  }

  type = "Opaque"
}

resource "helm_release" "lzy_server_db" {
  name       = "postgres"
  chart      = "postgresql"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name  = "global.postgresql.auth.database"
    value = "serverDB"
  }

  set {
    name  = "global.postgresql.auth.username"
    value = "server"
  }

  set {
    name  = "global.postgresql.auth.existingSecret"
    value = kubernetes_secret.lzy_server_db.metadata[0].name
  }

  set_sensitive {
    name  = "global.postgresql.auth.password"
    value = random_password.lzy_server_db_password[0].result
  }

  set {
    name  = "global.postgresql.service.ports.postgresql"
    value = "5432"
  }

  set {
    name  = "metadata.labels.app"
    value = "lzy-server-db"
  }

  values = [
    file("lzy_node_selector_and_pod_anti_affinity.yaml")
  ]
}
