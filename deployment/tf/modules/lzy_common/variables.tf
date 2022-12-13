variable "installation_name" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "kharon_public_ip" {
  default = ""
}

variable "create_public_kharon_service" {
  default = true
}

variable "backoffice_public_ip" {
  default = ""
}

variable "create_public_backoffice_service" {
  default = true
}

variable "grafana_public_ip" {
  default = ""
}

variable "create_public_grafana_service" {
  default = true
}

variable "kharon_load_balancer_necessary_annotations" {
  type    = map(string)
  default = {}
}

variable "grafana_load_balancer_necessary_annotations" {
  type    = map(string)
  default = {}
}

variable "backoffice_load_balancer_necessary_annotations" {
  type    = map(string)
  default = {}
}

// server db
variable "lzy_server_db_host" {
}

variable "lzy_server_db_port" {
}

variable "lzy_server_db_name" {
}

variable "lzy_server_db_user" {
}

variable "lzy_server_db_password" {
}

// whiteboard db
variable "lzy_whiteboard_db_host" {
}

variable "lzy_whiteboard_db_port" {
}

variable "lzy_whiteboard_db_name" {
}

variable "lzy_whiteboard_db_user" {
}

variable "lzy_whiteboard_db_password" {
}

// iam db
variable "iam_db_host" {
}

variable "iam_db_port" {
}

variable "iam_db_name" {
}

variable "iam_db_user" {
}

variable "iam_db_password" {
}

// docker images
variable "backoffice-frontend-image" {
  type    = string
  default = "lzydock/lzy-backoffice-frontend:master"
}

variable "backoffice-backend-image" {
  type    = string
  default = "lzydock/lzy-backoffice-backend:master"
}

variable "clickhouse-image" {
  type    = string
  default = "clickhouse/clickhouse-server"
}

variable "grafana-image" {
  type    = string
}

variable "kharon-image" {
  type    = string
}

variable "server-image" {
  type    = string
}

variable "whiteboard-image" {
  type    = string
}

variable "iam-image" {
  type    = string
}

variable "worker-image" {
  type = string
}

variable "default-env-image" {
  type    = string
}

variable "s3-bucket-name" {
  type    = string
  default = "lzy-bucket"
}

variable "s3-separated-per-bucket" {
  type    = bool
  default = true
}

variable "amazon-access-key" {
  type    = string
  default = ""
}

variable "amazon-secret-key" {
  type    = string
  default = ""
}

variable "amazon-service-endpoint" {
  type    = string
  default = ""
}

variable "azure-connection-string" {
  type    = string
  default = ""
}

variable "storage-provider" {
  type    = string
  default = "amazon"
}

variable "azure-resource-group" {
  type    = string
  default = ""
}

variable "ssl-enabled" {
  type    = bool
  default = false
}

variable "ssl-cert" {
  type    = string
  default = ""
}

variable "ssl-cert-key" {
  type    = string
  default = ""
}

variable "ssl-keystore-password" {
  type    = string
  default = ""
}

variable "kafka-url" {
  type    = string
  default = "kafka.default.svc.cluster.local:9092"
}

variable "server-additional-envs" {
  type    = map(string)
  default = {}
}
