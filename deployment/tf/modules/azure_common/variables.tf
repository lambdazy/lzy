variable "installation_name" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "cpu_pool_auto_scale" {
  default = true
}

variable "cpu_pool_size" {
  type    = number
  default = 5
}

variable "min_cpu_count" {
  type    = number
  default = 5
}

variable "max_cpu_count" {
  type    = number
  default = 10
}

variable "gpu_count" {
  type    = number
  default = 1
}

variable "create_public_kharon_service" {
  default = true
}

variable "create_public_backoffice_service" {
  default = true
}

variable "create_public_grafana_service" {
  default = true
}

variable "location" {
  type    = string
  default = "North Europe"
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

variable "s3-postfics" {
  type = string
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
variable "s3-separated-per-bucket" {
  type    = bool
  default = true
}
