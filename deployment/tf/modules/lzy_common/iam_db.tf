resource "random_password" "iam_db_password" {
  count   = 1
  length  = 16
  special = false
}

resource "kubernetes_secret" "iam_db" {
  metadata {
    name = "iam-db"
  }

  data = {
    postgresql-postgres-password = random_password.iam_db_password[0].result
    postgresql-password          = random_password.iam_db_password[0].result
    postgres-password            = random_password.iam_db_password[0].result
    password                     = random_password.iam_db_password[0].result
  }

  type = "Opaque"
}

resource "helm_release" "iam_db" {
  name       = "iam"
  chart      = "postgresql"
  repository = "https://charts.bitnami.com/bitnami"

  set {
    name  = "global.postgresql.auth.database"
    value = "iamDB"
  }

  set {
    name  = "global.postgresql.auth.username"
    value = "iam"
  }

  set {
    name  = "global.postgresql.auth.existingSecret"
    value = kubernetes_secret.iam_db.metadata[0].name
  }

  set_sensitive {
    name  = "global.postgresql.auth.password"
    value = random_password.iam_db_password[0].result
  }

  set {
    name  = "global.postgresql.service.ports.postgresql"
    value = "5432"
  }
}
