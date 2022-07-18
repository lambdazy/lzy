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

variable "servant-image" {
  type = string
}

variable "default-env-image" {
  type    = string
}
