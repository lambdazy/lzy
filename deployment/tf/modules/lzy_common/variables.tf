variable "installation_name" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "kharon_public_ip" {}

variable "backoffice_public_ip" {}

variable "kharon_load_balancer_necessary_annotations" {
  type    = map(string)
  default = {}
}

variable "backoffice_load_balancer_necessary_annotations" {
  type    = map(string)
  default = {}
}

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

variable "kharon-image" {
  type    = string
  default = "lzydock/lzy-kharon:master"
}

variable "server-image" {
  type    = string
  default = "lzydock/lzy-server:master"
}

variable "whiteboard-image" {
  type = string
  default = "lzydock/lzy-whiteboard:master"
}

variable "s3-bucket-name" {
  type = string
  default = "lzy-bucket"
}

variable "s3-access-key" {
  type = string
}

variable "s3-secret-key" {
  type = string
}

variable "s3-service-endpoint" {
  type = string
}

variable "s3-use-proxy" {
  type = string
  default = "false"
}

variable "s3-proxy-provider" {
  type = string
  default = ""
}

variable "azure-resource-group" {
  type = string
  default = ""
}

variable "cluster_id" {}
