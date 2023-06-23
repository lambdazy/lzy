variable "installation_name" {
  type    = string
}

//from cloud provider

variable "db-host" {
  type = string
}

variable "whiteboard_public_ip" {
  type = string
}

variable "workflow_public_ip" {
  type = string
}

variable "backoffice_public_ip" {
  type = string
}

variable "custom-subnet-id" {
  type = string
}

variable "node-sa-static-key-access-key" {
  type = string
}

variable "node-sa-static-key-secret-key" {
  type = string
}

variable "pool-cluster-id" {
  type = string
}

variable "service-cluster-id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "yc-endpoint" {
  type = string
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

variable "lzy-service-image" {
  type = string
}

variable "portal_image" {
  type = string
}

variable "backoffice-frontend-image" {
  type = string
}

variable "backoffice-backend-image" {
  type = string
}

variable "whiteboard-image" {
  type = string
}

variable "storage-image" {
  type = string
}

// Service config

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
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

variable "lzy_service_ip_families" {
  type = list(string)
  default = ["IPv4"]
}

variable "lzy_backoffice_ip_families" {
  type = list(string)
  default = ["IPv4"]
}

variable "whiteboard_service_ip_families" {
  type = list(string)
  default = ["IPv4"]
}

variable "allocator_service_ip_families" {
  type = list(string)
  default = ["IPv4"]
}

variable "channel_manager_service_ip_families" {
  type = list(string)
  default = ["IPv4"]
}

variable "iam_ip_families" {
  type = list(string)
  default = ["IPv4"]
}
