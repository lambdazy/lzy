variable "installation_name" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "agent_count" {
  type    = number
  default = 10
}

variable "cpu_count" {
  type    = number
  default = 7
}

variable "lzy_count" {
  type    = number
  default = 5
}

variable "gpu_count" {
  type    = number
  default = 1
}

variable "location" {
  type    = string
  default = "North Europe"
}

variable "backoffice-frontend-image" {
  type    = string
  default = "lzydock/lzy-backoffice-frontend:master"
}

variable "backoffice-backend-image" {
  type    = string
  default = "lzydock/lzy-backoffice-backend:master"
}

variable "servant-image" {
  type    = string
  default = "lzydock/lzy-servant:master"
}

variable "clickhouse-image" {
  type    = string
  default = "clickhouse/clickhouse-server"
}

variable "kharon-image" {
  type    = string
  default = "lzydock/lzy-kharon:master"
}

variable "server-image" {
  type    = string
  default = "lzydock/lzy-server:master"
}

variable "whiteboard-image" {
  type    = string
  default = "lzydock/lzy-whiteboard:master"
}
variable "s3-postfics" {
  type = string
}
variable "ssl-enabled" {
  type = bool
  default = false
}

variable "ssl-cert" {
  type = string
  default = ""
}

variable "ssl-cert-key" {
  type = string
  default = ""
}

variable "ssl-keystore-password" {
  type = string
  default = ""
}
