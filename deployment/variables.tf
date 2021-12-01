variable "installation_name" {
  type = string
}

variable "agent_count" {
  type = number
  default = 10
}

variable location {
  default = "North Europe"
}

variable "oauth-github-client-id" {
  type = string
}

variable "oauth-github-client-secret" {
  type = string
}

variable "backoffice-secrets-private-key" {
  type = string
}
