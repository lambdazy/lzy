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

variable "docker_images_tag" {
  type        = string
  description = "Docker image tag for services"
  default = "1.1"
}

variable "docker_worker_image_tag" {
  type        = string
  description = "Docker image tag for worker"
  default = "1.2"
}


variable "docker_unified_agent_tag" {
  type        = string
  description = "Docker image tag for unified agent"
  default = "1.0"
}