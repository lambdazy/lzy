#resource "random_password" "lzy_server_db_password" {
#  count   = 1
#  length  = 16
#  special = false
#}
#
#resource "kubernetes_secret" "lzy_server_db" {
#  metadata {
#    name      = "postgres"
#    namespace = kubernetes_namespace.server_namespace.metadata[0].name
#  }
#
#  data = {
#    postgresql-postgres-password = random_password.lzy_server_db_password[0].result
#    postgresql-password          = random_password.lzy_server_db_password[0].result
#    postgres-password            = random_password.lzy_server_db_password[0].result
#    password                     = random_password.lzy_server_db_password[0].result
#  }
#
#  type = "Opaque"
#}

#provider "postgresql" {
#  alias           = "lzy-server-postgresql"
#  host            = var.lzy_server_db_host
#  port            = var.lzy_server_db_port
#  database        = var.lzy_server_db_name
#  username        = var.lzy_server_db_user
#  password        = var.lzy_server_db_password
#  // TODO: think about ssl
#  sslmode         = "require"
#}
#
#data "postgresql_database" "lzy_server_db" {
#  provider  = "postgresql.lzy-server-postgresql"
#  name      = var.lzy_server_db_name
#}

#resource "helm_release" "lzy_server_db" {
#  name       = "postgres"
#  namespace  = kubernetes_namespace.server_namespace.metadata[0].name
#  chart      = "postgresql"
#  repository = "https://charts.bitnami.com/bitnami"
#
#  set {
#    name  = "global.postgresql.auth.database"
#    value = "serverDB"
#  }
#
#  set {
#    name  = "global.postgresql.auth.username"
#    value = "server"
#  }
#
#  set {
#    name  = "global.postgresql.auth.existingSecret"
#    value = kubernetes_secret.lzy_server_db.metadata[0].name
#  }
#
#  set_sensitive {
#    name  = "global.postgresql.auth.password"
#    value = random_password.lzy_server_db_password[0].result
#  }
#
#  set {
#    name  = "global.postgresql.service.ports.postgresql"
#    value = "5432"
#  }
#}
