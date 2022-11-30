variable "installation_name" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "cpu_pool_size" {
  type = number
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

variable "cloud_id" {
  type = string
}

variable "location" {
  type    = string
  default = "ru-central1-a"
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

variable "s3-separated-per-bucket" {
  type    = bool
  default = true
}

variable "ssl-enabled" {
  type    = bool
  default = false
}

variable "cluster-security-groups" {
  type = list(string)
  default = []
}

variable "nodes-security-groups" {
  type = list(string)
  default = []
}