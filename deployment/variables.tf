#variable "client_id" {}
#variable "client_secret" {}

variable "cluster_name" {
  type = string
  default = "lzy-testing"
}

variable "dns_prefix" {
  type = string
  default = "lzy-testing"
}

variable "agent_count" {
  type = number
  default = 10
}

variable location {
  default = "North Europe"
}

variable lzy_server_ip {
  default = "North Europe"
}

variable lzy_kharon_dns {
  default = "North Europe"
}
