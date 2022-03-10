resource "random_password" "whiteboard_db_password" {
  count   = 1
  length  = 16
  special = false
}

resource "kubernetes_secret" "whiteboard_db" {
  metadata {
    name = "whiteboard-db"
  }

  data = {
    postgresql-postgres-password = random_password.whiteboard_db_password[0].result
    postgresql-password          = random_password.whiteboard_db_password[0].result
    password                     = random_password.whiteboard_db_password[0].result
    database-name                = "whiteboards"
    username                     = "whiteboard"
  }

  type = "Opaque"
}

resource "helm_release" "whiteboard_db" {
  name       = "whiteboard"
  chart      = "postgresql"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name  = "global.postgresql.auth.database"
    value = kubernetes_secret.whiteboard_db.data.database-name
  }

  set {
    name  = "global.postgresql.auth.username"
    value = kubernetes_secret.whiteboard_db.data.username
  }

  set {
    name  = "global.postgresql.auth.existingSecret"
    value = kubernetes_secret.whiteboard_db.metadata[0].name
  }

  set_sensitive {
    name  = "global.postgresql.auth.password"
    value = random_password.whiteboard_db_password[0].result
  }

  set {
    name  = "global.postgresql.service.ports.postgresql"
    value = "5432"
  }
}
