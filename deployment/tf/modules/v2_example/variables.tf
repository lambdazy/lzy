variable "yc_token" {
  type        = string
  description = "Token for YC management"
  default     = null
  nullable    = true
  sensitive   = true
}

variable "oauth_github_client_id" {
  type        = string
  description = "Github OAuth client id"
}

variable "oauth_github_client_secret" {
  type        = string
  description = "Github OAuth client secret"
  sensitive   = true
}

variable "docker_iam_image_tag" {
  type        = string
  description = "Docker IAM image tag"
  default     = "1.0"
}

variable "docker_channel_manager_image_tag" {
  type        = string
  description = "Docker IAM channel-manager tag"
  default     = "1.0"
}

variable "docker_lzy_service_image_tag" {
  type        = string
  description = "Docker IAM lzy-service tag"
  default     = "1.1"
}

variable "docker_graph_executor_image_tag" {
  type        = string
  description = "Docker IAM graph-executor tag"
  default     = "1.1"
}

variable "docker_scheduler_image_tag" {
  type        = string
  description = "Docker IAM scheduler tag"
  default     = "1.0"
}

variable "docker_allocator_image_tag" {
  type        = string
  description = "Docker IAM allocator tag"
  default     = "1.1"
}

variable "docker_worker_image_tag" {
  type        = string
  description = "Docker IAM worker tag"
  default     = "1.2"
}

variable "docker_portal_image_tag" {
  type        = string
  description = "Docker IAM portal tag"
  default     = "1.1"
}

variable "docker_storage_image_tag" {
  type        = string
  description = "Docker IAM storage tag"
  default     = "1.0"
}

variable "docker_whiteboard_image_tag" {
  type        = string
  description = "Docker IAM whiteboard tag"
  default     = "1.0"
}

variable "docker_frontend_image_tag" {
  type        = string
  description = "Docker IAM frontend tag"
  default     = "1.0"
}

variable "docker_backend_image_tag" {
  type        = string
  description = "Docker IAM backend tag"
  default     = "1.1"
}

variable "docker_unified_agent_image_tag" {
  type        = string
  description = "Docker IAM unified agent tag"
  default     = "1.0"
}