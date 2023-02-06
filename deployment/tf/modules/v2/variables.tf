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

variable "graph-image" {
  type = string
}

variable "scheduler-image" {
  type = string
}

variable "servant-image" {
  type = string
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

variable "storage-image" {
  type = string
}

variable "whiteboard-image" {
  type = string
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "backoffice-frontend-image" {
  type = string
}

variable "backoffice-backend-image" {
  type = string
}

variable "unified-agent-image" {
  type = string
}