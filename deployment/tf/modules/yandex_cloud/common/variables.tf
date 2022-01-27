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

variable "cloud_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "location" {
  type    = string
  default = "ru-central1-a"
}

variable "network_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "YC_TOKEN" {
  description = "YC IAM token"
}

variable "servant-image" {
  type    = string
  default = "lzydock/lzy-servant:master"
}

variable "server-image" {
  type    = string
  default = "lzydock/lzy-server:master"
}

variable "s3-separated-per-bucket" {
  type    = bool
  default = true
}