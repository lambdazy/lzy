variable "installation_name" {
  type = string
}

variable "agent_count" {
  type = number
  default = 10
}

variable "cpu_count" {
  type = number
  default = 7
}

variable "lzy_count" {
  type = number
  default = 5
}

variable "gpu_count" {
  type = number
  default = 2
}

variable location {
  default = "North Europe"
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "backoffice-frontend-image" {
  type = string
  default = "lzydock/lzy-backoffice-frontend:master"
}

variable "backoffice-backend-image" {
  type = string
  default = "lzydock/lzy-backoffice-backend:master"
}

variable "clickhouse-image" {
  type = string
  default = "clickhouse/clickhouse-server"
}

variable "kharon-image" {
  type = string
  default = "lzydock/lzy-kharon:master"
}

variable "server-image" {
  type = string
  default = "lzydock/lzy-server:master"
}
