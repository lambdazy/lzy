variable "installation_name" {
  type    = string
  default = "dev"
}

// docker images

variable "iam-image" {
  type = string
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

variable "yc-endpoint" {
  type = string
}

variable "zone" {
  type    = string
  default = "ru-central1-a"
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

variable "ssl-enabled" {
  type    = bool
  default = false
}

variable "ssl-keystore-password" {
  type    = string
  default = ""
}

variable "workers_nodegroups_definition" {
  type = map(object({
    platform_id = string
    kind        = string # CPU | GPU
    resource_spec = object({
      cores  = number
      gpus   = optional(number, 0)
      memory = number
    })
    scale_policy = object({
      fixed        = bool
      initial_size = number
      min_size     = optional(number, 0)
      max_size     = optional(number, 0)
    })
  }))
  validation {
    condition     = length(var.workers_nodegroups_definition) > 0
    error_message = "Amount of workers node-groups cannot be zero"
  }
}

variable "portals_pool_size" {
  type = number
}

variable "domain_name" {
  type     = string
  default  = "lzy.ai"
  nullable = true
}

variable "enable_kafka" {
  type = bool
  default = true
}

variable "s3_sink_enabled" {
  type = bool
  default = true
}

variable "s3-sink-image" {
  type = string
}

variable "node-sync-image" {
  type = string
  default = "lzydock/kuber-node-sync:1.0"
}
