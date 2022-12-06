variable "installation_name" {
  type = string
  default = "dev"
}

// docker images

variable "iam-image" {
  type    = string
}

variable "allocator-image" {
  type = string
}

variable "channel-manager-image" {
  type = string
}

variable "scheduler-image" {
  type = string
}

variable "servant-image" {
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

variable "network_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "user-clusters" {
  type = list(string)
}

variable "yc-endpoint" {
  type = string
}

variable "zone" {
  type = string
  default = "ru-central1-a"
}

variable "max-servants-per-workflow" {
  type = number
}

variable "worker-limits-by-labels" {
  type = map(number)
}
