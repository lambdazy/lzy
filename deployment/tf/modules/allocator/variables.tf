variable "cloud_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "zone" {
  type = string
  default = "ru-central1-a"
}

variable "installation_name" {
  type = string
  default = "lzy-allocator"
}

variable "network_id" {
  type = string
}

variable "iam-image" {
  type = string
}

variable "allocator-image" {
  type = string
}

variable "subnet-id" {
  type = string
}

variable "subnet-folder" {
  type = string
}

variable "yc-token" {
  type = string
}

variable "yc-endpoint" {
  type = string
}

variable "user-clusters" {
  type = list(string)
}
